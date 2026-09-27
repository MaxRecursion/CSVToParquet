using DuckDB.NET.Data;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

namespace CsvToParquet.Api.Tests;

/// <summary>Hosts the API with its own unique temp WorkDirectory, which is deleted on dispose.</summary>
public sealed class TestApp : WebApplicationFactory<Program>
{
    public long? MaxUploadBytes { get; init; }

    /// <summary>Extra <c>Conversion:*</c> settings, for example <c>["MaxConcurrentRequests"] = "1"</c>.</summary>
    public Dictionary<string, string> Settings { get; init; } = [];

    public string WorkDirectory { get; } = Path.Combine(Path.GetTempPath(), "csv2parquet-tests", Guid.NewGuid().ToString("N"));

    /// <summary>DuckDB's per-process spill folders, which exist for as long as the app does.</summary>
    public string[] SpillDirectories =>
        Directory.Exists(WorkDirectory) ? Directory.GetDirectories(WorkDirectory, $"{ParquetConverter.SpillDirectoryPrefix}*") : [];

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting("Conversion:WorkDirectory", WorkDirectory);
        if (MaxUploadBytes is long max)
        {
            builder.UseSetting("Conversion:MaxUploadBytes", max.ToString());
        }

        foreach (var (key, value) in Settings)
        {
            builder.UseSetting($"Conversion:{key}", value);
        }
    }

    /// <summary>T5: waits up to 2 s (the Parquet is deleted when the response stream closes) and returns what is left.</summary>
    /// <remarks>A spill folder always exists while the app runs, so its contents are reported instead of the folder.</remarks>
    public async Task<string[]> WaitForEmptyWorkDirectoryAsync()
    {
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (true)
        {
            var entries = Leftovers();
            if (entries.Length == 0 || DateTime.UtcNow >= deadline)
            {
                return entries;
            }

            await Task.Delay(50);
        }

        string[] Leftovers()
        {
            var spill = SpillDirectories;
            return
            [
                .. Directory.GetFileSystemEntries(WorkDirectory).Except(spill),
                .. spill.SelectMany(Directory.GetFileSystemEntries)
            ];
        }
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        if (Directory.Exists(WorkDirectory))
        {
            Directory.Delete(WorkDirectory, recursive: true);
        }
    }

    public static long CountParquetRows(string parquetPath) =>
        Convert.ToInt64(Scalar($"SELECT count(*) FROM read_parquet('{Escape(parquetPath)}')"));

    /// <summary>The codecs the file's column chunks were written with, for example <c>ZSTD</c> or <c>SNAPPY</c>.</summary>
    public static string ParquetCompression(string parquetPath) =>
        DistinctValues("compression", $"parquet_metadata('{Escape(parquetPath)}')");

    /// <summary>The values in the file's <c>tag</c> column: one tag unless a conversion mixed up two requests.</summary>
    public static string ParquetTags(string parquetPath) =>
        DistinctValues("tag", $"read_parquet('{Escape(parquetPath)}')");

    /// <summary>Every distinct value of <paramref name="column"/>, comma-separated, so an unexpected extra one fails the assertion.</summary>
    private static string DistinctValues(string column, string from) =>
        (string)Scalar($"SELECT string_agg({column}, ',') FROM (SELECT DISTINCT {column} FROM {from} ORDER BY {column})")!;

    public static Dictionary<string, string> ParquetColumnTypes(string parquetPath)
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{Escape(parquetPath)}'))";
        using var reader = command.ExecuteReader();
        var types = new Dictionary<string, string>();
        while (reader.Read())
        {
            types[reader.GetString(0)] = reader.GetString(1);
        }

        return types;
    }

    /// <summary>Reads a whole CSV with DuckDB; throws <see cref="DuckDBException"/> if DuckDB rejects it.</summary>
    public static long CountCsvRows(string csvPath) =>
        Convert.ToInt64(Scalar($"SELECT count(*) FROM read_csv('{Escape(csvPath)}')"));

    private static object? Scalar(string sql)
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    private static string Escape(string value) => value.Replace("'", "''");
}
