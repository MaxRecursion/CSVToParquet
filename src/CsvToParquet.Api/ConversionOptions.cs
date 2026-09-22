namespace CsvToParquet.Api;

/// <summary>Settings bound from the <c>Conversion</c> configuration section.</summary>
public sealed class ConversionOptions
{
    public const string SectionName = "Conversion";

    private static readonly string DefaultWorkDirectory = Path.Combine(Path.GetTempPath(), "csv2parquet");

    /// <summary>Local disk for temp CSV, Parquet and spill files. Empty means the default temp folder.</summary>
    public string WorkDirectory
    {
        get;
        set => field = string.IsNullOrWhiteSpace(value) ? DefaultWorkDirectory : value;
    } = DefaultWorkDirectory;

    /// <summary>DuckDB conversions at once.</summary>
    public int MaxConcurrentConversions { get; set; } = 2;

    /// <summary>Per conversion; keep threads × concurrency ≈ CPU cores.</summary>
    public int DuckDbThreads { get; set; } = 4;

    /// <summary>Per conversion; limit × concurrency must fit in RAM.</summary>
    public string DuckDbMemoryLimit { get; set; } = "4GB";

    /// <summary>Set to false if a huge file runs out of memory.</summary>
    public bool PreserveRowOrder { get; set; } = true;

    /// <summary>Maximum upload size in bytes; null = no limit.</summary>
    public long? MaxUploadBytes { get; set; }
}
