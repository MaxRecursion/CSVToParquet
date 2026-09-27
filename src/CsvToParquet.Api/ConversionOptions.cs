using System.Text.RegularExpressions;

namespace CsvToParquet.Api;

/// <summary>Settings bound from the <c>Conversion</c> configuration section.</summary>
public sealed class ConversionOptions
{
    public const string SectionName = "Conversion";

    private static readonly string DefaultWorkDirectory = Path.Combine(Path.GetTempPath(), "csv2parquet");

    /// <summary>Parquet codecs DuckDB accepts. Also an allow-list, because the value is written into SQL.</summary>
    private static readonly string[] Codecs = ["uncompressed", "snappy", "gzip", "zstd", "brotli", "lz4", "lz4_raw"];

    /// <summary>A DuckDB memory limit such as <c>4GB</c>, <c>512MiB</c> or <c>1.5 GB</c>.</summary>
    private static readonly Regex MemoryLimitPattern =
        new(@"^\d+(\.\d+)?\s*(b|[kmgt]b|[kmgt]ib)$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>Local disk for temp CSV, Parquet and spill files. Empty means the default temp folder.</summary>
    public string WorkDirectory
    {
        get;
        set => field = string.IsNullOrWhiteSpace(value) ? DefaultWorkDirectory : value;
    } = DefaultWorkDirectory;

    /// <summary>DuckDB conversions at once; the rest queue. 0 = one per CPU core, at least 2 and at most 8.</summary>
    public int MaxConcurrentConversions { get; set; }

    /// <summary>Requests admitted to <c>/api/convert</c> at once; the rest queue. 0 = no limit.</summary>
    public int MaxConcurrentRequests { get; set; }

    /// <summary>Requests queued once <see cref="MaxConcurrentRequests"/> is reached; beyond it, 503. null = as many as are admitted.</summary>
    public int? MaxQueuedRequests { get; set; }

    /// <summary>Seconds a request waits for a conversion slot before giving up with 503. 0 = wait as long as the client does.</summary>
    public int ConversionQueueTimeoutSeconds { get; set; }

    /// <summary>Worker threads shared by every conversion, not per conversion. 0 = one per CPU core.</summary>
    public int DuckDbThreads { get; set; }

    /// <summary>Memory shared by every conversion, not per conversion. Empty = DuckDB's own default (about 80% of RAM).</summary>
    public string DuckDbMemoryLimit { get; set; } = "";

    /// <summary>Setting this to false is the single biggest speed-up, at the cost of reordering rows.</summary>
    public bool PreserveRowOrder { get; set; } = true;

    /// <summary>Parquet codec: uncompressed, snappy, gzip, zstd, brotli, lz4 or lz4_raw. snappy is faster, zstd is smaller.</summary>
    public string ParquetCompression { get; set; } = "zstd";

    /// <summary>Codec level, 1 = fastest. null = the codec's default (3 for zstd).</summary>
    public int? ParquetCompressionLevel { get; set; }

    /// <summary>Rows per Parquet row group. null = DuckDB's default (122880).</summary>
    public long? ParquetRowGroupSize { get; set; }

    /// <summary>Maximum upload size in bytes; null = no limit.</summary>
    public long? MaxUploadBytes { get; set; }

    /// <summary><see cref="MaxConcurrentConversions"/> with 0 resolved to one per CPU core.</summary>
    public int EffectiveMaxConcurrentConversions =>
        MaxConcurrentConversions > 0 ? MaxConcurrentConversions : Math.Clamp(Environment.ProcessorCount, 2, 8);

    /// <summary><see cref="DuckDbThreads"/> with 0 resolved to one per CPU core.</summary>
    public int EffectiveDuckDbThreads => DuckDbThreads > 0 ? DuckDbThreads : Environment.ProcessorCount;

    /// <summary><see cref="MaxQueuedRequests"/> with null resolved to <see cref="MaxConcurrentRequests"/>.</summary>
    public int EffectiveMaxQueuedRequests => MaxQueuedRequests ?? MaxConcurrentRequests;

    /// <summary>Fails fast on a setting that would produce invalid SQL or an unusable limit.</summary>
    public void Validate()
    {
        if (!Codecs.Contains(ParquetCompression, StringComparer.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Conversion:ParquetCompression '{ParquetCompression}' must be one of: {string.Join(", ", Codecs)}.");
        }

        if (ParquetCompressionLevel is int level)
        {
            if (level is < 1 or > 22)
            {
                throw new InvalidOperationException($"Conversion:ParquetCompressionLevel {level} must be between 1 and 22.");
            }

            // DuckDB: "Compression level is only supported for the ZSTD compression codec".
            if (!ParquetCompression.Equals("zstd", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Conversion:ParquetCompressionLevel only applies to zstd, not '{ParquetCompression}'. Leave it null.");
            }
        }

        if (ParquetRowGroupSize is long rowGroupSize and < 1)
        {
            throw new InvalidOperationException($"Conversion:ParquetRowGroupSize {rowGroupSize} must be at least 1.");
        }

        if (DuckDbMemoryLimit.Length > 0 && !MemoryLimitPattern.IsMatch(DuckDbMemoryLimit))
        {
            throw new InvalidOperationException(
                $"Conversion:DuckDbMemoryLimit '{DuckDbMemoryLimit}' must be a size such as '4GB' or '512MiB', or empty for DuckDB's default.");
        }

        if (MaxConcurrentRequests < 0)
        {
            throw new InvalidOperationException($"Conversion:MaxConcurrentRequests {MaxConcurrentRequests} cannot be negative (0 = no limit).");
        }

        if (MaxQueuedRequests is int queued and < 0)
        {
            throw new InvalidOperationException($"Conversion:MaxQueuedRequests {queued} cannot be negative.");
        }

        if (ConversionQueueTimeoutSeconds < 0)
        {
            throw new InvalidOperationException(
                $"Conversion:ConversionQueueTimeoutSeconds {ConversionQueueTimeoutSeconds} cannot be negative (0 = no timeout).");
        }
    }
}
