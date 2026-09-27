using System.Diagnostics;
using DuckDB.NET.Data;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace CsvToParquet.Api;

public static class ConvertEndpoint
{
    /// <summary>Rate-limiter policy that caps how many requests convert at once (<see cref="ConversionOptions.MaxConcurrentRequests"/>).</summary>
    public const string RateLimiterPolicy = "convert";

    private const string ParquetContentType = "application/vnd.apache.parquet";

    /// <summary>503 with <c>Retry-After</c>, for both ways the service sheds load: too many requests, or no conversion slot in time.</summary>
    public static IResult ServerBusy(HttpContext http, string detail)
    {
        http.Response.Headers.RetryAfter = "5";
        return Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable, title: "Server busy", detail: detail);
    }

    public static IEndpointRouteBuilder MapConvertEndpoint(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/convert", ConvertAsync)
            .WithName("ConvertCsvToParquet")
            .WithSummary("Convert a CSV file to Parquet")
            .WithDescription("""
                The request body is the raw CSV file with `Content-Type: text/csv`, not multipart/form-data.
                The response body is the Parquet file (zstd-compressed, column types detected automatically).

                Requests are served in parallel. `Conversion:MaxConcurrentRequests` caps how many are admitted at
                once and `Conversion:MaxConcurrentConversions` caps how many convert at once; the rest queue.

                Large files: upload with a streaming client, for example curl `-T`:

                ```
                curl -T data.csv -X POST -H "Content-Type: text/csv" http://localhost:<port>/api/convert -o data.parquet
                ```

                Swagger UI is only suitable for small test files, because the browser holds the whole response in memory.
                """)
            .Accepts<Stream>("text/csv")
            .Produces<Stream>(StatusCodes.Status200OK, ParquetContentType)
            .ProducesProblem(StatusCodes.Status400BadRequest)
            .ProducesProblem(StatusCodes.Status413PayloadTooLarge)
            .ProducesProblem(StatusCodes.Status422UnprocessableEntity)
            .ProducesProblem(StatusCodes.Status500InternalServerError)
            .ProducesProblem(StatusCodes.Status503ServiceUnavailable)
            .ProducesProblem(StatusCodes.Status507InsufficientStorage)
            .RequireRateLimiting(RateLimiterPolicy);
        return app;
    }

    private static async Task<IResult> ConvertAsync(
        HttpContext http,
        Stream body,
        [FromServices] ParquetConverter converter,
        [FromServices] IOptions<ConversionOptions> options,
        [FromServices] ILoggerFactory loggerFactory)
    {
        // 1. Kestrel and IIS both default to ~30 MB; lift the limit for this request before touching the body.
        var limit = http.Features.Get<IHttpMaxRequestBodySizeFeature>();
        if (limit is { IsReadOnly: false }) limit.MaxRequestBodySize = options.Value.MaxUploadBytes;

        // 2. The CSV, the Parquet and DuckDB's spill files all land on the WorkDirectory drive.
        var workDirectory = options.Value.WorkDirectory;
        var contentLength = http.Request.ContentLength;
        if (contentLength is long length)
        {
            var freeBytes = new DriveInfo(Path.GetPathRoot(Path.GetFullPath(workDirectory))!).AvailableFreeSpace;
            if (freeBytes < 2 * length)
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status507InsufficientStorage,
                    title: "Insufficient storage",
                    detail: $"The server needs about {2 * length} bytes of free disk space to convert this file but has {freeBytes}.");
            }
        }

        var logger = loggerFactory.CreateLogger(typeof(ConvertEndpoint).FullName!);
        var id = Guid.NewGuid().ToString("N");
        var csvPath = Path.Combine(workDirectory, $"{id}.csv");
        var parquetPath = Path.Combine(workDirectory, $"{id}.parquet");
        try
        {
            // 3. Stream the upload to disk; it is never held in memory. Uploads are not capped by the conversion
            // limit, so a queued request keeps using the network while the ones ahead of it convert.
            var uploadStart = Stopwatch.GetTimestamp();
            var fso = new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Options = FileOptions.Asynchronous,
                BufferSize = 0, // CopyToAsync already writes 1 MB at a time; FileStream must not copy it again
                PreallocationSize = contentLength ?? 0 // reserves disk space up front
            };
            long csvBytes;
            await using (var csv = new FileStream(csvPath, fso))
            {
                // Throws if the upload is cut short, so a partial file never reaches DuckDB.
                await body.CopyToAsync(csv, 1024 * 1024, http.RequestAborted);
                csvBytes = csv.Length;
            } // must be closed before DuckDB opens it (Windows file locking)
            var uploadMs = (long)Stopwatch.GetElapsedTime(uploadStart).TotalMilliseconds;

            // 4. Nothing to convert.
            if (csvBytes == 0)
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Empty request body",
                    detail: "Send the CSV file as the raw request body with Content-Type: text/csv.");
            }

            // 5. Convert; DuckDB errors are either server-side (500) or a bad CSV (422).
            ConversionTimings? timings;
            try
            {
                timings = await converter.ConvertAsync(csvPath, parquetPath, http.RequestAborted);
            }
            catch (DuckDBException ex)
            {
                File.Delete(parquetPath);
                if (ex.Message.StartsWith("Out of Memory Error", StringComparison.Ordinal) ||
                    ex.Message.StartsWith("IO Error", StringComparison.Ordinal))
                {
                    logger.LogError(ex, "Conversion {Id} failed on the server", id);
                    return Results.Problem(
                        statusCode: StatusCodes.Status500InternalServerError,
                        title: "Conversion failed on the server",
                        detail: ex.Message);
                }

                logger.LogWarning("Conversion {Id} rejected the CSV: {Message}", id, ex.Message);
                return Results.Problem(
                    statusCode: StatusCodes.Status422UnprocessableEntity,
                    title: "The CSV could not be converted",
                    detail: ex.Message);
            }
            catch
            {
                File.Delete(parquetPath);
                throw;
            }

            // 6. Every conversion slot was busy for longer than the client should wait.
            if (timings is not ConversionTimings elapsed)
            {
                logger.LogWarning("Conversion {Id} gave up after {Seconds} s waiting for a conversion slot",
                    id, options.Value.ConversionQueueTimeoutSeconds);
                return ServerBusy(http,
                    $"All {options.Value.EffectiveMaxConcurrentConversions} conversion slots were busy for "
                    + $"{options.Value.ConversionQueueTimeoutSeconds} seconds. Retry later or raise Conversion:MaxConcurrentConversions.");
            }

            // 7. Stream the Parquet back from disk. A seekable stream makes ASP.NET Core send Content-Length,
            // and DeleteOnClose removes the file when the response ends.
            var parquet = new FileStream(parquetPath, new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.DeleteOnClose
            });

            // 10. One line per conversion.
            logger.LogInformation(
                "Converted {Id}: {CsvBytes} CSV bytes -> {ParquetBytes} Parquet bytes, upload {UploadMs} ms, queued {QueueWaitMs} ms, conversion {ConversionMs} ms",
                id, csvBytes, parquet.Length, uploadMs, elapsed.QueueWaitMs, elapsed.ConversionMs);

            return Results.File(parquet, ParquetContentType, "converted.parquet");
        }
        catch (Exception ex) when (ex is (OperationCanceledException or IOException) && http.RequestAborted.IsCancellationRequested)
        {
            // 9. The client went away: clean up (finally) and return quietly. Anything else propagates,
            // including BadHttpRequestException, which the server turns into 400/413 itself.
            return Results.Empty;
        }
        finally
        {
            // 8. The Parquet deletes itself when its stream closes.
            File.Delete(csvPath);
        }
    }
}
