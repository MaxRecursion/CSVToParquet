using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using Microsoft.AspNetCore.Mvc;

namespace CsvToParquet.Api.Tests;

/// <summary>T9-T13: requests convert in parallel, and the configurable limits shed load instead of piling it up.</summary>
public sealed class ParallelRequestTests
{
    /// <summary>
    /// Settings that make one conversion take seconds without a big file: zstd at its slowest level on a single
    /// thread. They let a test hold the only conversion slot long enough to be sure another request queued behind it.
    /// </summary>
    private static readonly Dictionary<string, string> SlowConversion = new()
    {
        ["DuckDbThreads"] = "1",
        ["ParquetCompression"] = "zstd",
        ["ParquetCompressionLevel"] = "22"
    };

    /// <summary>About 5 MB of incompressible text, which <see cref="SlowConversion"/> turns into several seconds of work.</summary>
    private const int SlowRows = 60_000;

    [Fact]
    public async Task T9_ConcurrentRequests_EachGetItsOwnParquet()
    {
        const int requests = 8;
        await using var app = new TestApp { Settings = { ["MaxConcurrentConversions"] = requests.ToString() } };
        using var client = app.CreateClient();

        // Every request sends a different number of rows and its own tag, so a mixed-up response cannot pass.
        var responses = await Task.WhenAll(Enumerable.Range(1, requests).Select(async i =>
        {
            using var response = await client.PostAsync("/api/convert", CsvContent(Csv(rows: i * 10, tag: $"req-{i}")));
            return (Request: i, response.StatusCode, Parquet: await response.Content.ReadAsByteArrayAsync());
        }));

        foreach (var (request, status, parquet) in responses)
        {
            Assert.Equal(HttpStatusCode.OK, status);
            var path = await SaveAsync(parquet);
            try
            {
                Assert.Equal(request * 10, TestApp.CountParquetRows(path));
                Assert.Equal($"req-{request}", TestApp.ParquetTags(path));
            }
            finally
            {
                File.Delete(path);
            }
        }

        Assert.Empty(await app.WaitForEmptyWorkDirectoryAsync());
    }

    [Fact]
    public async Task T10_OverMaxConcurrentRequests_Returns503WithRetryAfter()
    {
        await using var app = new TestApp
        {
            Settings = new(SlowConversion) { ["MaxConcurrentRequests"] = "1", ["MaxQueuedRequests"] = "0" }
        };
        using var client = app.CreateClient();
        var csv = Csv(rows: 20_000, tag: "load");

        var statuses = await Task.WhenAll(Enumerable.Range(0, 12).Select(async _ =>
        {
            using var response = await client.PostAsync("/api/convert", CsvContent(csv));
            if (response.StatusCode is not HttpStatusCode.ServiceUnavailable)
            {
                return response.StatusCode;
            }

            Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
            Assert.Equal(TimeSpan.FromSeconds(5), response.Headers.RetryAfter?.Delta);
            var problem = await response.Content.ReadFromJsonAsync<ProblemDetails>();
            Assert.Contains("MaxConcurrentRequests", problem?.Detail ?? "", StringComparison.Ordinal);
            return response.StatusCode;
        }));

        // One permit and no queue: requests that find it taken are shed rather than left to pile up on disk.
        Assert.Contains(HttpStatusCode.ServiceUnavailable, statuses);
        Assert.Contains(HttpStatusCode.OK, statuses);
        Assert.All(statuses, status => Assert.Contains(status, (HttpStatusCode[])[HttpStatusCode.OK, HttpStatusCode.ServiceUnavailable]));
        Assert.Empty(await app.WaitForEmptyWorkDirectoryAsync());
    }

    [Fact]
    public async Task T11_NoConversionSlotWithinTheTimeout_Returns503()
    {
        await using var app = new TestApp
        {
            Settings = new(SlowConversion) { ["MaxConcurrentConversions"] = "1", ["ConversionQueueTimeoutSeconds"] = "1" }
        };
        using var client = app.CreateClient();

        // Take the only conversion slot and hold it for several seconds.
        var held = client.PostAsync("/api/convert", CsvContent(Csv(SlowRows, tag: "slot-holder")));
        await WaitUntilConvertingAsync(app);

        using var queued = await client.PostAsync("/api/convert", CsvContent(Csv(rows: 10, tag: "queued")));

        Assert.False(held.IsCompleted, "The slot holder finished too early for this test to prove anything.");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, queued.StatusCode);
        Assert.Equal(TimeSpan.FromSeconds(5), queued.Headers.RetryAfter?.Delta);
        var problem = await queued.Content.ReadFromJsonAsync<ProblemDetails>();
        Assert.Contains("MaxConcurrentConversions", problem?.Detail ?? "", StringComparison.Ordinal);

        using var holder = await held;
        Assert.Equal(HttpStatusCode.OK, holder.StatusCode);
        var path = await SaveAsync(await holder.Content.ReadAsByteArrayAsync());
        try
        {
            Assert.Equal(SlowRows, TestApp.CountParquetRows(path));
        }
        finally
        {
            File.Delete(path);
        }

        Assert.Empty(await app.WaitForEmptyWorkDirectoryAsync());
    }

    [Fact]
    public async Task T12_ClientDisconnects_FreesTheSlotAndLeavesNothingBehind()
    {
        await using var app = new TestApp
        {
            Settings = new(SlowConversion) { ["MaxConcurrentConversions"] = "1", ["ConversionQueueTimeoutSeconds"] = "30" }
        };
        using var client = app.CreateClient();

        using var abort = new CancellationTokenSource();
        var abandoned = client.PostAsync("/api/convert", CsvContent(Csv(SlowRows, tag: "abandoned")), abort.Token);
        await WaitUntilConvertingAsync(app);
        await abort.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned);

        // The CSV and the half-written Parquet are gone, and the interrupted conversion gave its slot back:
        // without the DuckDB interrupt this request would sit in the queue for the full 30 s and then return 503.
        Assert.Empty(await app.WaitForEmptyWorkDirectoryAsync());
        using var next = await client.PostAsync("/api/convert", CsvContent(Csv(rows: 5, tag: "after-abort")));
        Assert.Equal(HttpStatusCode.OK, next.StatusCode);
    }

    [Fact]
    public async Task T13_ParquetCompression_IsConfigurable()
    {
        await using var app = new TestApp { Settings = { ["ParquetCompression"] = "snappy" } };
        using var client = app.CreateClient();

        using var response = await client.PostAsync("/api/convert", CsvContent(Csv(rows: 100, tag: "snappy")));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var path = await SaveAsync(await response.Content.ReadAsByteArrayAsync());
        try
        {
            Assert.Equal("SNAPPY", TestApp.ParquetCompression(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>Returns once DuckDB has started writing a Parquet file, which means a conversion slot is taken.</summary>
    private static async Task WaitUntilConvertingAsync(TestApp app)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (Directory.GetFiles(app.WorkDirectory, "*.parquet").Length == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "No conversion started within 30 s.");
            await Task.Delay(20);
        }
    }

    /// <summary>A CSV whose <c>tag</c> column identifies the request, padded with text zstd cannot compress cheaply.</summary>
    private static byte[] Csv(int rows, string tag)
    {
        var random = new Random(tag.Length * 31 + rows);
        var csv = new StringBuilder("id,amount,day,tag,padding\n");
        var padding = new char[64];
        for (var i = 1; i <= rows; i++)
        {
            for (var c = 0; c < padding.Length; c++)
            {
                padding[c] = (char)('a' + random.Next(26));
            }

            csv.Append(CultureInfo.InvariantCulture, $"{i},{i * 1.25m:F2},2024-01-{i % 28 + 1:D2},{tag},{new string(padding)}\n");
        }

        return Encoding.UTF8.GetBytes(csv.ToString());
    }

    private static async Task<string> SaveAsync(byte[] parquet)
    {
        var path = Path.Combine(Path.GetTempPath(), $"parallel-{Guid.NewGuid():N}.parquet");
        await File.WriteAllBytesAsync(path, parquet);
        return path;
    }

    private static ByteArrayContent CsvContent(byte[] bytes)
    {
        var content = new ByteArrayContent(bytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("text/csv");
        return content;
    }
}
