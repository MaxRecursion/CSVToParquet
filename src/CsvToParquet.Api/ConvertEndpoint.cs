using System.Diagnostics;
using DuckDB.NET.Data;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace CsvToParquet.Api;

public static class ConvertEndpoint
{
    private const string ParquetContentType = "application/vnd.apache.parquet";

    public static IEndpointRouteBuilder MapConvertEndpoint(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/convert", ConvertAsync)
            .WithName("ConvertCsvToParquet")
            .WithSummary("Convert a CSV file to Parquet")
            .WithDescription("""
                The request body is the raw CSV file with `Content-Type: text/csv`, not multipart/form-data.
                The response body is the Parquet file (zstd-compressed, column types detected automatically).

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
            .ProducesProblem(StatusCodes.Status507InsufficientStorage);
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
            // 3. Stream the upload to disk; it is never held in memory.
            var stopwatch = Stopwatch.StartNew();
            var fso = new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Options = FileOptions.Asynchronous,
                PreallocationSize = contentLength ?? 0 // reserves disk space up front
            };
            long csvBytes;
            await using (var csv = new FileStream(csvPath, fso))
            {
                // Throws if the upload is cut short, so a partial file never reaches DuckDB.
                await body.CopyToAsync(csv, 1024 * 1024, http.RequestAborted);
                csvBytes = csv.Length;
            } // must be closed before DuckDB opens it (Windows file locking)
            var uploadMs = stopwatch.ElapsedMilliseconds;

            // 4. Nothing to convert.
            if (csvBytes == 0)
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Empty request body",
                    detail: "Send the CSV file as the raw request body with Content-Type: text/csv.");
            }

            // 5. Convert; DuckDB errors are either server-side (500) or a bad CSV (422).
            stopwatch.Restart();
            try
            {
                await converter.ConvertAsync(csvPath, parquetPath, http.RequestAborted);
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
            var conversionMs = stopwatch.ElapsedMilliseconds;

            // 6. Stream the Parquet back from disk. A seekable stream makes ASP.NET Core send Content-Length,
            // and DeleteOnClose removes the file when the response ends.
            var parquet = new FileStream(parquetPath, new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.DeleteOnClose
            });

            // 9. One line per conversion.
            logger.LogInformation(
                "Converted {Id}: {CsvBytes} CSV bytes -> {ParquetBytes} Parquet bytes, upload {UploadMs} ms, conversion {ConversionMs} ms",
                id, csvBytes, parquet.Length, uploadMs, conversionMs);

            return Results.File(parquet, ParquetContentType, "converted.parquet");
        }
        catch (Exception ex) when (ex is (OperationCanceledException or IOException) && http.RequestAborted.IsCancellationRequested)
        {
            // 8. The client went away: clean up (finally) and return quietly. Anything else propagates,
            // including BadHttpRequestException, which the server turns into 400/413 itself.
            return Results.Empty;
        }
        finally
        {
            // 7. The Parquet deletes itself when its stream closes.
            File.Delete(csvPath);
        }
    }
}
