using System.Diagnostics;
using DuckDB.NET.Data;
using Microsoft.Extensions.Options;

namespace CsvToParquet.Api;

/// <summary>How long a conversion spent queued and how long it then took.</summary>
public readonly record struct ConversionTimings(long QueueWaitMs, long ConversionMs);

/// <summary>
/// Converts CSV files to Parquet with a single DuckDB <c>COPY</c>, at most
/// <see cref="ConversionOptions.MaxConcurrentConversions"/> at once.
/// </summary>
/// <remarks>
/// One warm in-memory DuckDB instance is shared by every conversion. Creating an instance costs about 15 ms and
/// duplicating a connection to it about 0.3 ms, so requests no longer pay for start-up; more importantly, DuckDB's
/// own scheduler then spreads <see cref="ConversionOptions.DuckDbThreads"/> worker threads across whatever
/// conversions are running, instead of each conversion guessing its share and oversubscribing the CPU.
/// </remarks>
public sealed class ParquetConverter : IDisposable
{
    /// <summary>Prefix of the folder under the WorkDirectory that DuckDB spills to when a conversion outgrows memory.</summary>
    /// <remarks>
    /// One folder per process, not one per conversion: a single DuckDB instance manages its own spill files, and an
    /// overlapped IIS app-pool recycle briefly runs two worker processes against the same WorkDirectory, so a shared
    /// name would let the starting process wipe the spill files the draining one is still using. Folders left by a
    /// crash are removed by the WorkDirectory clean-up at start-up once they are a day old.
    /// </remarks>
    public const string SpillDirectoryPrefix = "spill-";

    private readonly ConversionOptions _options;
    private readonly ILogger<ParquetConverter> _logger;
    private readonly SemaphoreSlim _slots;
    private readonly int _slotWaitMs;
    private readonly string _copyOptions;

    /// <summary>Holds the shared in-memory database open: it is dropped when its last connection closes.</summary>
    private readonly DuckDBConnection _database;

    public ParquetConverter(IOptions<ConversionOptions> options, ILogger<ParquetConverter> logger)
    {
        _options = options.Value;
        _options.Validate();
        _logger = logger;
        _slots = new SemaphoreSlim(_options.EffectiveMaxConcurrentConversions);
        _slotWaitMs = _options.ConversionQueueTimeoutSeconds > 0 ? _options.ConversionQueueTimeoutSeconds * 1000 : Timeout.Infinite;
        _copyOptions = BuildCopyOptions(_options);

        var spillDirectory = Path.Combine(
            Path.GetFullPath(_options.WorkDirectory),
            $"{SpillDirectoryPrefix}{Environment.ProcessId}");
        Directory.CreateDirectory(spillDirectory);

        // Opened here rather than on the first request, so no request pays for DuckDB's start-up.
        _database = new DuckDBConnection("Data Source=:memory:");
        _database.Open();
        using var command = _database.CreateCommand();
        command.CommandText = $"""
            SET threads = {_options.EffectiveDuckDbThreads};
            {(_options.DuckDbMemoryLimit.Length > 0 ? $"SET memory_limit = '{Escape(_options.DuckDbMemoryLimit)}';" : "")}
            SET temp_directory = '{Escape(spillDirectory)}';
            SET preserve_insertion_order = {(_options.PreserveRowOrder ? "true" : "false")};
            SET autoinstall_known_extensions = false;
            SET autoload_known_extensions = false;
            """;
        command.ExecuteNonQuery();

        _logger.LogInformation(
            "DuckDB ready: {Conversions} concurrent conversions, {Threads} worker threads, memory limit {MemoryLimit}, spill {SpillDirectory}",
            _options.EffectiveMaxConcurrentConversions,
            _options.EffectiveDuckDbThreads,
            _options.DuckDbMemoryLimit.Length > 0 ? _options.DuckDbMemoryLimit : "DuckDB default",
            spillDirectory);
    }

    /// <summary>Converts <paramref name="csvPath"/> to <paramref name="parquetPath"/>, or returns null if no conversion slot came free in time.</summary>
    public async Task<ConversionTimings?> ConvertAsync(string csvPath, string parquetPath, CancellationToken ct)
    {
        var queued = Stopwatch.StartNew();
        if (!await _slots.WaitAsync(_slotWaitMs, ct))
        {
            return null;
        }

        var queueWaitMs = queued.ElapsedMilliseconds;
        var conversion = Stopwatch.StartNew();
        try
        {
            await RunCopyAsync(
                $"COPY (SELECT * FROM read_csv('{Escape(csvPath)}')) TO '{Escape(parquetPath)}' ({_copyOptions});",
                ct);
        }
        finally
        {
            _slots.Release();
        }

        return new ConversionTimings(queueWaitMs, conversion.ElapsedMilliseconds);
    }

    public void Dispose()
    {
        _database.Dispose();
        _slots.Dispose();
    }

    /// <summary>
    /// Runs the <c>COPY</c> on a thread of its own, and cancels it if <paramref name="ct"/> fires.
    /// </summary>
    /// <remarks>
    /// DuckDB has no asynchronous API, so <c>ExecuteNonQuery</c> blocks for the whole conversion — minutes for a
    /// multi-GB file. On a thread-pool thread that would starve the pool of the threads the other parallel requests
    /// need for their uploads and downloads, because the pool only grows by about one thread per second. A dedicated
    /// thread costs microseconds and the semaphore bounds how many exist. <c>Cancel()</c> then stops a conversion
    /// whose client has gone away within a few milliseconds, which frees the slot for a request that is still there.
    /// </remarks>
    private Task RunCopyAsync(string sql, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thread = new Thread(() =>
        {
            try
            {
                using var connection = _database.Duplicate();
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = sql;

                // Disposing the registration waits for a callback that is already running, so the command outlives it.
                using (ct.Register(() => Interrupt(command)))
                {
                    // A token that fired while the slot was being acquired interrupted nothing, because there was no
                    // query yet. Check again here so an abandoned conversion is never started in the first place.
                    ct.ThrowIfCancellationRequested();
                    command.ExecuteNonQuery();
                }

                completion.SetResult();
            }
            catch (Exception ex)
            {
                completion.SetException(ex);
            }
        })
        {
            IsBackground = true,
            Name = "csv2parquet-convert"
        };

        thread.Start();
        return completion.Task;
    }

    /// <summary>Stops a running <c>COPY</c>. Interrupting one that has not started, or has already finished, is a no-op.</summary>
    private void Interrupt(DuckDBCommand command)
    {
        try
        {
            command.Cancel();
        }
        catch (DuckDBException ex)
        {
            // This runs on whichever thread aborted the request, so it must not throw there. A conversion that
            // cannot be interrupted simply runs to completion and its result is thrown away.
            _logger.LogDebug(ex, "Could not interrupt a conversion");
        }
    }

    private static string BuildCopyOptions(ConversionOptions options)
    {
        // ParquetCompression is checked against an allow-list by Validate(), so it is safe unquoted.
        var parts = new List<string> { "FORMAT parquet", $"COMPRESSION {options.ParquetCompression.ToLowerInvariant()}" };
        if (options.ParquetCompressionLevel is int level)
        {
            parts.Add($"COMPRESSION_LEVEL {level}");
        }

        if (options.ParquetRowGroupSize is long rowGroupSize)
        {
            parts.Add($"ROW_GROUP_SIZE {rowGroupSize}");
        }

        return string.Join(", ", parts);
    }

    private static string Escape(string value) => value.Replace("'", "''");
}
