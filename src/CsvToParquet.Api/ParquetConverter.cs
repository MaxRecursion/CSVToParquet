using DuckDB.NET.Data;
using Microsoft.Extensions.Options;

namespace CsvToParquet.Api;

/// <summary>Converts a CSV file to Parquet with a single DuckDB <c>COPY</c>, at most <see cref="ConversionOptions.MaxConcurrentConversions"/> at once.</summary>
public sealed class ParquetConverter(IOptions<ConversionOptions> options)
{
    private readonly ConversionOptions _options = options.Value;
    private readonly SemaphoreSlim _semaphore = new(options.Value.MaxConcurrentConversions);

    public async Task ConvertAsync(string csvPath, string parquetPath, CancellationToken ct)
    {
        await _semaphore.WaitAsync(ct);

        // {WorkDirectory}/{id}.tmp: concurrent DuckDB instances can collide in a shared spill folder.
        var spillDir = Path.ChangeExtension(parquetPath, ".tmp");
        try
        {
            using var connection = new DuckDBConnection("Data Source=:memory:");
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = $"""
                SET threads = {_options.DuckDbThreads};
                SET memory_limit = '{Escape(_options.DuckDbMemoryLimit)}';
                SET temp_directory = '{Escape(spillDir)}';
                SET preserve_insertion_order = {(_options.PreserveRowOrder ? "true" : "false")};
                SET autoinstall_known_extensions = false;
                SET autoload_known_extensions = false;
                COPY (SELECT * FROM read_csv('{Escape(csvPath)}'))
                TO '{Escape(parquetPath)}' (FORMAT parquet, COMPRESSION zstd);
                """;

            // Synchronous on purpose: the semaphore bounds how many run at once.
            command.ExecuteNonQuery();
        }
        finally
        {
            _semaphore.Release();
            if (Directory.Exists(spillDir))
            {
                Directory.Delete(spillDir, recursive: true);
            }
        }
    }

    private static string Escape(string value) => value.Replace("'", "''");
}
