using System.Globalization;
using System.Net;
using System.Net.Http.Headers;

namespace CsvToParquet.Api.Tests;

/// <summary>Real Kestrel: the in-memory TestServer does not enforce Kestrel's ~30 MB body size limit.</summary>
[Trait("Category", "LargeFile")]
public sealed class LargeFileTests
{
    [Fact]
    public async Task T6_100MbCsv_StreamsThroughKestrel()
    {
        await using var app = new TestApp();
        using var client = StartKestrel(app);

        var folder = Path.Combine(Path.GetTempPath(), $"csv2parquet-t6-{Guid.NewGuid():N}");
        Directory.CreateDirectory(folder);
        try
        {
            var csvPath = Path.Combine(folder, "large.csv");
            var parquetPath = Path.Combine(folder, "large.parquet");
            var rows = await WriteCsvAsync(csvPath, 100L * 1024 * 1024);

            HttpStatusCode status;
            long? contentLength;
            await using (var csv = File.OpenRead(csvPath))
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, "/api/convert") { Content = new StreamContent(csv) };
                request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/csv");

                using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
                status = response.StatusCode;
                contentLength = response.Content.Headers.ContentLength;
                await using var parquet = File.Create(parquetPath);
                await response.Content.CopyToAsync(parquet);
            }

            Assert.Equal(HttpStatusCode.OK, status);
            Assert.NotNull(contentLength);
            Assert.Equal(contentLength, new FileInfo(parquetPath).Length);
            Assert.Equal(rows, TestApp.CountParquetRows(parquetPath));
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    [Fact]
    public async Task T7_UploadOverMaxUploadBytes_Returns413()
    {
        await using var app = new TestApp { MaxUploadBytes = 1024 * 1024 };
        using var client = StartKestrel(app);

        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/convert") { Content = new ByteArrayContent(new byte[2 * 1024 * 1024]) };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/csv");
        request.Headers.ExpectContinue = true;

        using var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
    }

    private static HttpClient StartKestrel(TestApp app)
    {
        app.UseKestrel(0); // dynamic port; must come before StartServer()/CreateClient()
        app.ClientOptions.AllowAutoRedirect = false; // the redirect handler would copy the request body into memory
        app.StartServer();
        return app.CreateClient();
    }

    private static async Task<long> WriteCsvAsync(string path, long targetBytes)
    {
        await using var writer = new StreamWriter(path);
        await writer.WriteLineAsync("id,amount,day,label");
        long rows = 0;
        while (writer.BaseStream.Length < targetBytes)
        {
            for (var i = 0; i < 10_000; i++)
            {
                rows++;
                await writer.WriteLineAsync(string.Create(CultureInfo.InvariantCulture, $"{rows},{rows * 1.25m:F2},2024-01-{rows % 28 + 1:D2},label-{rows % 1000}-abcdefghijklmnop"));
            }

            await writer.FlushAsync();
        }

        return rows;
    }
}
