using CsvToParquet.Api;
using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    // As a Windows Service the working directory is System32; pin the content root to the app folder.
    ContentRootPath = WindowsServiceHelpers.IsWindowsService() ? AppContext.BaseDirectory : default
});
builder.Host.UseWindowsService(); // no-op unless started by the Service Control Manager

builder.Services.Configure<ConversionOptions>(builder.Configuration.GetSection(ConversionOptions.SectionName));
builder.Services.AddSingleton<ParquetConverter>();
builder.Services.AddOpenApi();

var app = builder.Build();

PrepareWorkDirectory(app.Services.GetRequiredService<IOptions<ConversionOptions>>().Value.WorkDirectory, app.Logger);

// Internal tool: the OpenAPI document and Swagger UI are served in every environment.
app.MapOpenApi();
app.UseSwaggerUI(o => o.SwaggerEndpoint("/openapi/v1.json", "CSV to Parquet v1"));
app.MapGet("/", () => Results.Redirect("/swagger")).ExcludeFromDescription();

app.MapConvertEndpoint();

app.Run();

// Fails fast unless the WorkDirectory is on a local, writable drive, then removes leftovers from crashes.
static void PrepareWorkDirectory(string workDirectory, ILogger logger)
{
    var fullPath = Path.GetFullPath(workDirectory);
    try
    {
        _ = new DriveInfo(Path.GetPathRoot(fullPath)!);
    }
    catch (Exception ex)
    {
        throw new InvalidOperationException($"Conversion:WorkDirectory '{fullPath}' must be on a local drive. {ex.Message}", ex);
    }

    try
    {
        Directory.CreateDirectory(fullPath);
        var probe = Path.Combine(fullPath, $"probe-{Guid.NewGuid():N}");
        File.WriteAllText(probe, string.Empty);
        File.Delete(probe);
    }
    catch (Exception ex)
    {
        throw new InvalidOperationException($"Conversion:WorkDirectory '{fullPath}' is not writable. {ex.Message}", ex);
    }

    var cutoff = DateTime.UtcNow.AddHours(-24);
    foreach (var entry in new DirectoryInfo(fullPath).EnumerateFileSystemInfos())
    {
        if (entry.LastWriteTimeUtc >= cutoff)
        {
            continue;
        }

        try
        {
            if (entry is DirectoryInfo directory)
            {
                directory.Delete(recursive: true);
            }
            else
            {
                entry.Delete();
            }

            logger.LogInformation("Removed stale work item {Path}", entry.FullName);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            logger.LogWarning(ex, "Could not remove stale work item {Path}", entry.FullName);
        }
    }

    logger.LogInformation("WorkDirectory: {Path}", fullPath);
}
