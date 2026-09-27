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

    /// <summary>How often a cancelled conversion is interrupted again until its <c>COPY</c> returns.</summary>
    private const int InterruptRetryMs = 50;

    private readonly ConversionOptions _options;
    private readonly ILogger<ParquetConverter> _logger;
    private readonly SemaphoreSlim _slots;
    private readonly int _slotWaitMs;
    private readonly string _copyOptions;
    private readonly string _spillDirectory;

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

        // No live process can share our id, so an existing folder was left by one that crashed.
        _spillDirectory = Path.Combine(Path.GetFullPath(_options.WorkDirectory), $"{SpillDirectoryPrefix}{Environment.ProcessId}");
        if (Directory.Exists(_spillDirectory))
        {
            Directory.Delete(_spillDirectory, recursive: true);
        }

        Directory.CreateDirectory(_spillDirectory);

        // Opened here rather than on the first request, so no request pays for DuckDB's start-up.
        _database = new DuckDBConnection("Data Source=:memory:");
        _database.Open();
        Execute($"""
            SET threads = {_options.EffectiveDuckDbThreads};
            SET temp_directory = '{Escape(_spillDirectory)}';
            SET preserve_insertion_order = {(_options.PreserveRowOrder ? "true" : "false")};
            SET autoinstall_known_extensions = false;
            SET autoload_known_extensions = false;
            """);

        // DuckDB is the authority on what it accepts ('4GB', '8G', '512MiB', 'none', ...), so ask it rather than
        // guess. The value is a quoted, escaped literal, so it cannot change the statement.
        if (_options.DuckDbMemoryLimit.Length > 0)
        {
            try
            {
                Execute($"SET memory_limit = '{Escape(_options.DuckDbMemoryLimit)}';");
            }
            catch (DuckDBException ex)
            {
                Dispose();
                throw new InvalidOperationException(
                    $"Conversion:DuckDbMemoryLimit '{_options.DuckDbMemoryLimit}' is not a size DuckDB accepts, such as '4GB', '512MiB' or 'none'. {ex.Message}",
                    ex);
            }
        }

        _logger.LogInformation(
            "DuckDB ready: {Conversions} concurrent conversions, {Threads} worker threads, memory limit {MemoryLimit}, spill {SpillDirectory}",
            _options.EffectiveMaxConcurrentConversions,
            _options.EffectiveDuckDbThreads,
            _options.DuckDbMemoryLimit.Length > 0 ? _options.DuckDbMemoryLimit : "DuckDB default",
            _spillDirectory);
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

    /// <remarks>
    /// <c>_slots</c> is deliberately not disposed: a conversion that outlives the host's shutdown timeout still calls
    /// <c>Release()</c>, which would throw on a disposed semaphore, and a SemaphoreSlim holds nothing that needs freeing
    /// unless its wait handle is used.
    /// </remarks>
    public void Dispose()
    {
        // Closing the last connection makes DuckDB delete its spill files, but not the folder, which it did not create.
        _database.Dispose();
        try
        {
            Directory.Delete(_spillDirectory, recursive: false);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Still in use by a conversion that outlived shutdown; the start-up clean-up removes it within a day.
        }
    }

    /// <summary>
    /// Runs the <c>COPY</c> on a thread of its own, and cancels it if <paramref name="ct"/> fires.
    /// </summary>
    /// <remarks>
    /// DuckDB has no asynchronous API, so <c>ExecuteNonQuery</c> blocks for the whole conversion — minutes for a
    /// multi-GB file. On a thread-pool thread that would starve the pool of the threads the other parallel requests
    /// need for their uploads and downloads, because the pool only grows by about one thread per second. A dedicated
    /// thread costs microseconds and the semaphore bounds how many exist. A conversion whose client has gone away
    /// is interrupted, which frees the slot for a request that is still there. DuckDB acts on an interrupt between
    /// row groups, so it stops once the row group being written is done: measured over a 100 MB CSV at the default
    /// settings, 57 ms typical and 617 ms worst.
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
                ExecuteInterruptibly(command, ct);
                completion.SetResult();
            }
            catch (Exception) when (ct.IsCancellationRequested)
            {
                // An interrupt that lands while DuckDB is still binding the statement surfaces as a DuckDBException
                // ("INTERRUPT Error: Interrupted!"), not OperationCanceledException. It must not reach the endpoint
                // as one, or a client that went away would be logged as having sent a bad CSV.
                completion.SetCanceled(ct);
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

    /// <summary>Runs <paramref name="command"/>, interrupting it every 50 ms from the moment <paramref name="ct"/> fires until it returns.</summary>
    /// <remarks>
    /// <c>Cancel()</c> only interrupts a statement DuckDB has already started. One sent before <c>ExecuteNonQuery</c>,
    /// or in roughly its first 100 microseconds, is silently dropped and the <c>COPY</c> runs to completion, holding its
    /// slot for a client that is gone. Repeating the interrupt until the statement returns closes that window.
    /// </remarks>
    private void ExecuteInterruptibly(DuckDBCommand command, CancellationToken ct)
    {
        var interrupter = new Timer(_ => Interrupt(command));
        try
        {
            // Disposing the registration waits for a callback that is already running, so the timer outlives it.
            using (ct.Register(() => interrupter.Change(dueTime: 0, period: InterruptRetryMs)))
            {
                ct.ThrowIfCancellationRequested();
                command.ExecuteNonQuery();
            }
        }
        finally
        {
            // Waits for an interrupt that is already running, so none reaches the command after it is disposed.
            using var stopped = new ManualResetEvent(initialState: false);
            if (interrupter.Dispose(stopped))
            {
                stopped.WaitOne();
            }
        }
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
            // This runs on a timer thread, so it must not throw there. The next retry tries again.
            _logger.LogDebug(ex, "Could not interrupt a conversion");
        }
    }

    private void Execute(string sql)
    {
        using var command = _database.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
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
