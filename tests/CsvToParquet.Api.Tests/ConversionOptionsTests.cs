namespace CsvToParquet.Api.Tests;

/// <summary>T8: the configurable limits and their auto-sized defaults, without starting the app.</summary>
public sealed class ConversionOptionsTests
{
    [Fact]
    public void ShippedDefaultsAreValidAndScaleWithTheMachine()
    {
        var options = new ConversionOptions();

        options.Validate();

        Assert.Equal(Math.Clamp(Environment.ProcessorCount, 2, 8), options.EffectiveMaxConcurrentConversions);
        Assert.Equal(Environment.ProcessorCount, options.EffectiveDuckDbThreads);
        Assert.Equal(0, options.MaxConcurrentRequests); // no limit
        Assert.Equal(0, options.EffectiveMaxQueuedRequests);
        Assert.Equal(0, options.ConversionQueueTimeoutSeconds); // no timeout
        Assert.Equal(Path.Combine(Path.GetTempPath(), "csv2parquet"), options.WorkDirectory);
    }

    [Fact]
    public void ExplicitLimitsWin()
    {
        var options = new ConversionOptions
        {
            MaxConcurrentConversions = 32,
            MaxConcurrentRequests = 6,
            DuckDbThreads = 3,
            DuckDbMemoryLimit = "1.5GiB"
        };

        options.Validate();

        Assert.Equal(32, options.EffectiveMaxConcurrentConversions);
        Assert.Equal(3, options.EffectiveDuckDbThreads);
        Assert.Equal(6, options.EffectiveMaxQueuedRequests); // null queue = as many as are admitted
        Assert.Equal(0, new ConversionOptions { MaxConcurrentRequests = 6, MaxQueuedRequests = 0 }.EffectiveMaxQueuedRequests);
    }

    [Fact]
    public void BlankWorkDirectoryFallsBackToTheDefault()
    {
        Assert.Equal(new ConversionOptions().WorkDirectory, new ConversionOptions { WorkDirectory = "   " }.WorkDirectory);
        Assert.Equal("/data/csv2parquet", new ConversionOptions { WorkDirectory = "/data/csv2parquet" }.WorkDirectory);
    }

    [Theory]
    // A codec outside the allow-list never reaches SQL.
    [InlineData(nameof(ConversionOptions.ParquetCompression), "zstd'); DROP TABLE x; --")]
    [InlineData(nameof(ConversionOptions.ParquetCompression), "bzip2")]
    [InlineData(nameof(ConversionOptions.ParquetCompressionLevel), 0)]
    [InlineData(nameof(ConversionOptions.ParquetCompressionLevel), 23)]
    [InlineData(nameof(ConversionOptions.ParquetRowGroupSize), 0L)]
    [InlineData(nameof(ConversionOptions.DuckDbMemoryLimit), "lots")]
    [InlineData(nameof(ConversionOptions.DuckDbMemoryLimit), "4GB'; SET threads = 1; --")]
    [InlineData(nameof(ConversionOptions.MaxConcurrentRequests), -1)]
    [InlineData(nameof(ConversionOptions.MaxQueuedRequests), -1)]
    [InlineData(nameof(ConversionOptions.ConversionQueueTimeoutSeconds), -1)]
    public void UnusableSettingsFailFast(string setting, object value)
    {
        var options = new ConversionOptions();
        typeof(ConversionOptions).GetProperty(setting)!.SetValue(options, value);

        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

        Assert.Contains(setting, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CompressionLevelOnlyAppliesToZstd()
    {
        var options = new ConversionOptions { ParquetCompression = "snappy", ParquetCompressionLevel = 5 };

        // DuckDB itself rejects this combination: "Compression level is only supported for the ZSTD compression codec".
        var ex = Assert.Throws<InvalidOperationException>(options.Validate);

        Assert.Contains("only applies to zstd", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("4GB")]
    [InlineData("512MiB")]
    [InlineData("1.5 GB")]
    [InlineData("")]
    public void MemoryLimitsDuckDbUnderstandsAreAccepted(string limit) =>
        new ConversionOptions { DuckDbMemoryLimit = limit }.Validate();
}
