using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace CsvToParquet.Api.Tests;

/// <summary>T14: the converter on its own: start-up checks, cancellation at any moment, and its spill folder.</summary>
public sealed class ParquetConverterTests : IDisposable
{
    private readonly string _workDirectory = Path.Combine(Path.GetTempPath(), "csv2parquet-tests", Guid.NewGuid().ToString("N"));

    public ParquetConverterTests() => Directory.CreateDirectory(_workDirectory);

    public void Dispose() => Directory.Delete(_workDirectory, recursive: true);

    [Theory]
    [InlineData("")] // DuckDB's own default
    [InlineData("4GB")]
    [InlineData("8G")]
    [InlineData("512MiB")]
    [InlineData("1.5 GB")]
    [InlineData("8 gigabytes")]
    [InlineData("none")]
    [InlineData("-1")]
    public void MemoryLimitsDuckDbAccepts_Start(string limit)
    {
        using var converter = Converter(o => o.DuckDbMemoryLimit = limit);
    }

    [Theory]
    [InlineData("lots")]
    [InlineData("4")] // no unit
    [InlineData("80%")]
    [InlineData("4GB'; SET threads = 1; --")] // stays inside the quoted literal, where DuckDB rejects it
    public void MemoryLimitsDuckDbRejects_FailAtStartup(string limit)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => Converter(o => o.DuckDbMemoryLimit = limit));

        Assert.Contains("Conversion:DuckDbMemoryLimit", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CancelledConversion_StopsWithOperationCanceled_WheneverTheCancelLands()
    {
        // One slot, and a COPY that takes about 3 s: zstd at its slowest level on one thread. DuckDB only acts on an
        // interrupt between row groups, so small ones keep it responsive; with one big row group a cancel would
        // still win, but only once that row group had been compressed.
        using var converter = Converter(o =>
        {
            o.MaxConcurrentConversions = 1;
            o.ConversionQueueTimeoutSeconds = 10;
            o.DuckDbThreads = 1;
            o.ParquetCompressionLevel = 22;
            o.ParquetRowGroupSize = 4096;
        });
        var csv = WriteCsv("slow.csv", rows: 150_000);

        // From "already cancelled" to well into the COPY. A single Cancel() sent in roughly the first 100 us of
        // ExecuteNonQuery is dropped by DuckDB, and one sent while the statement is still being bound surfaces as a
        // DuckDBException; both used to leak out as a conversion that ran to completion or as a 422.
        foreach (var delayUs in new[] { -1, 0, 10, 25, 50, 100, 200, 500, 2_000, 20_000 })
        {
            using var cancel = new CancellationTokenSource();
            if (delayUs < 0)
            {
                await cancel.CancelAsync();
            }

            var conversion = converter.ConvertAsync(csv, Path.Combine(_workDirectory, $"out{delayUs}.parquet"), cancel.Token);
            if (delayUs >= 0)
            {
                var spin = Stopwatch.StartNew();
                while (spin.Elapsed.TotalMicroseconds < delayUs)
                {
                }

                await cancel.CancelAsync();
            }

            // Measured: 20 ms typical, 84 ms worst. A lost cancel would instead run the full ~3 s and not throw.
            var stopping = Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => conversion);
            Assert.True(stopping.ElapsedMilliseconds < 1_000, $"Cancelled after {delayUs} us, but took {stopping.ElapsedMilliseconds} ms to stop.");
        }

        // Every cancelled conversion gave the only slot back.
        var small = WriteCsv("small.csv", rows: 10);
        Assert.NotNull(await converter.ConvertAsync(small, Path.Combine(_workDirectory, "small.parquet"), CancellationToken.None));
        Assert.Equal(10, TestApp.CountParquetRows(Path.Combine(_workDirectory, "small.parquet")));
    }

    [Fact]
    public void SpillFolder_IsClearedAtStartupAndRemovedOnDispose()
    {
        // Only a process that crashed can have left a folder with our process id on it.
        var spill = Path.Combine(_workDirectory, $"{ParquetConverter.SpillDirectoryPrefix}{Environment.ProcessId}");
        Directory.CreateDirectory(spill);
        File.WriteAllText(Path.Combine(spill, "left-by-a-crash.block"), "stale");

        var converter = Converter();
        Assert.Empty(Directory.GetFileSystemEntries(spill));

        converter.Dispose();
        Assert.False(Directory.Exists(spill));
    }

    private ParquetConverter Converter(Action<ConversionOptions>? configure = null)
    {
        var options = new ConversionOptions { WorkDirectory = _workDirectory };
        configure?.Invoke(options);
        return new ParquetConverter(Options.Create(options), NullLogger<ParquetConverter>.Instance);
    }

    /// <summary>A CSV of random text, which zstd cannot compress cheaply.</summary>
    private string WriteCsv(string name, int rows)
    {
        var random = new Random(rows);
        var csv = new StringBuilder("id,padding\n");
        var padding = new char[64];
        for (var i = 1; i <= rows; i++)
        {
            for (var c = 0; c < padding.Length; c++)
            {
                padding[c] = (char)('a' + random.Next(26));
            }

            csv.Append(CultureInfo.InvariantCulture, $"{i},{new string(padding)}\n");
        }

        var path = Path.Combine(_workDirectory, name);
        File.WriteAllText(path, csv.ToString());
        return path;
    }
}
