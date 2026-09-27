# plan.md: CSV → Parquet conversion service

## 1. Goal

Build a small internal ASP.NET Core (.NET 10) minimal-API service with one endpoint, `POST /api/convert`, that takes raw CSV bytes and returns the Parquet file, converting synchronously in the request with DuckDB and bounded memory. Multi-GB uploads and downloads are the main use case, so the request is streamed to a temp file, DuckDB converts it with one `COPY` statement, and the Parquet is streamed back from a self-deleting temp file. The same build runs under IIS in-process (up to ~4 GB) or as a Windows Service on Kestrel (no size cap).

## 2. Environment

| Item | Value (from Phase 0) |
|---|---|
| .NET SDK used | **10.0.302**, pinned with `global.json` (see note) |
| Other SDKs installed | 11.0.100-preview.3.26207.106, 11.0.100-preview.6.26359.118 |
| Runtimes | Microsoft.NETCore.App 10.0.10, Microsoft.AspNetCore.App 10.0.10 (plus 11.0 previews) |
| NuGet feeds | 1 feed only: `nuget.org` [Enabled], https://api.nuget.org/v3/index.json |
| `.github/copilot-instructions.md` | Not present, so default .NET conventions apply |
| Workspace | Empty before Phase 1 (no project files) |
| Dev OS | macOS (Darwin 25.6.0). IIS and Windows Service modes can only be checked here by build/publish, not by running them. |

Note: without a `global.json`, `dotnet --version` in this folder resolves to `11.0.100-preview.6.26359.118`. A root `global.json` pins `10.0.302` (`rollForward: latestFeature`, which stays on 10.0.x) so the build and templates use the .NET 10 SDK.

## 3. Locked decisions

1. ASP.NET Core minimal API on .NET 10 with one endpoint: `POST /api/convert`. No controllers.
2. The request body is the raw CSV bytes (`Content-Type: text/csv`), not multipart. `IFormFile` buffers the whole upload before our code runs and has a 128 MB default limit.
3. The upload is streamed straight to a temp file on local disk. It is never held in memory.
4. DuckDB (`DuckDB.NET.Data.Full`) converts CSV → Parquet with a single `COPY` statement. It gives parallel parsing, automatic type detection and bounded memory. No other CSV/Parquet library, no hand-written parsing.
5. Conversion runs synchronously inside the request; the response body is the Parquet file. No job queue, polling or background services.
6. The Parquet is streamed back from a temp file opened with `FileOptions.DeleteOnClose`, so it is deleted when the response ends. It is never held in memory.
7. At most `MaxConcurrentConversions` DuckDB conversions run at once (a `SemaphoreSlim`). Uploads are not limited.
8. OpenAPI document from `Microsoft.AspNetCore.OpenApi`; Swagger UI from `Swashbuckle.AspNetCore.SwaggerUI` (UI package only). Never add `Swashbuckle.AspNetCore`, `AddSwaggerGen` or `UseSwagger`.
9. One build supports two hosting modes: IIS in-process (uploads up to ~4 GB, a hard IIS limit) and a Windows Service running Kestrel (no size cap).

Revised in Phase 9 (see the deviations log):

10. One warm in-memory DuckDB instance is shared by every conversion; each conversion runs on a connection duplicated from it. `threads`, `memory_limit`, `temp_directory` and `preserve_insertion_order` are DuckDB globals, so they are totals for the service and are applied once at startup.
11. `COPY` runs on a dedicated thread, not a thread-pool thread, and is interrupted with `DuckDBCommand.Cancel()` when the request is aborted.
12. Two configurable limits, not one: `MaxConcurrentRequests` (+ `MaxQueuedRequests`) admits requests through an ASP.NET Core concurrency rate limiter, and `MaxConcurrentConversions` (+ `ConversionQueueTimeoutSeconds`) caps conversions. Both shed load with 503 and `Retry-After`.

## 4. File layout

| File | Purpose |
|---|---|
| `global.json` | Pins the .NET SDK to 10.0.302 (the default here is an 11.0 preview) |
| `CsvToParquet.slnx` | Solution from `dotnet new sln -n CsvToParquet` (SDK 10 produces `.slnx`) |
| `.gitignore` | From `dotnet new gitignore` |
| `Directory.Build.props` | `Nullable` enable, `ImplicitUsings` enable, `TreatWarningsAsErrors` true for all projects |
| `plan.md` | This contract |
| `README.md` | Running locally, config, curl and HttpClient streaming examples, deployment notes |
| `src/CsvToParquet.Api/CsvToParquet.Api.csproj` | Web project (`dotnet new web`), `net10.0`, the 4 API packages |
| `src/CsvToParquet.Api/Program.cs` | Host setup (IIS or Windows Service), options and DI, WorkDirectory validation and cleanup, OpenAPI, Swagger UI, `/` redirect, maps the endpoint |
| `src/CsvToParquet.Api/ConversionOptions.cs` | Options class bound from the `Conversion` section, with defaults |
| `src/CsvToParquet.Api/ParquetConverter.cs` | Singleton: semaphore plus a DuckDB `COPY` into Parquet in a per-conversion spill folder |
| `src/CsvToParquet.Api/ConvertEndpoint.cs` | `MapConvertEndpoint`: body limit, disk pre-check, stream to disk, convert, stream Parquet back, error mapping, logging, OpenAPI metadata |
| `src/CsvToParquet.Api/appsettings.json` | Every `Conversion` setting with its default, plus the template's logging settings |
| `src/CsvToParquet.Api/appsettings.Development.json` | Template file (kept as generated) |
| `src/CsvToParquet.Api/Properties/launchSettings.json` | Template file (kept as generated) |
| `src/CsvToParquet.Api/web.config` | IIS: ANCM V2 in-process, `maxAllowedContentLength=4294967295`, URL compression off |
| `tests/CsvToParquet.Api.Tests/CsvToParquet.Api.Tests.csproj` | xUnit project (`dotnet new xunit`), references the API and `Microsoft.AspNetCore.Mvc.Testing` |
| `tests/CsvToParquet.Api.Tests/TestApp.cs` | `WebApplicationFactory<Program>` subclass with its own unique temp WorkDirectory and option overrides, plus DuckDB Parquet-reading helpers |
| `tests/CsvToParquet.Api.Tests/ConvertApiTests.cs` | In-memory TestServer tests T1–T5 (one class, so it runs sequentially) |
| `tests/CsvToParquet.Api.Tests/LargeFileTests.cs` | Real-Kestrel tests T6–T7, `[Trait("Category", "LargeFile")]` |

The template's `UnitTest1.cs` is deleted. Nothing else is added.

## 5. Tasks

### Phase 2: Scaffold
- [x] 2.1 Create `global.json` pinning SDK `10.0.302` (`rollForward: latestFeature`); verify `dotnet --version` prints `10.0.x`.
- [x] 2.2 `dotnet new sln -n CsvToParquet` and `dotnet new gitignore` in the root.
- [x] 2.3 `dotnet new web -o src/CsvToParquet.Api -f net10.0`; verify `<TargetFramework>net10.0</TargetFramework>`.
- [x] 2.4 `dotnet new xunit -o tests/CsvToParquet.Api.Tests -f net10.0`; add a project reference to the API and the `Microsoft.AspNetCore.Mvc.Testing` package (latest stable 10.x).
- [x] 2.5 Add both projects to the solution.
- [x] 2.6 Create root `Directory.Build.props` (`Nullable`=enable, `ImplicitUsings`=enable, `TreatWarningsAsErrors`=true); remove the now-duplicate `Nullable`/`ImplicitUsings` lines from both csproj files.
- [x] 2.7 Add the API packages, latest stable for net10.0, from nuget.org only: `DuckDB.NET.Data.Full`, `Microsoft.AspNetCore.OpenApi`, `Swashbuckle.AspNetCore.SwaggerUI`, `Microsoft.Extensions.Hosting.WindowsServices`. Record the resolved versions in the final summary.
- [x] 2.8 `dotnet build` gives 0 warnings and 0 errors.

### Phase 3: Options and converter
- [x] 3.1 `ConversionOptions.cs`: `WorkDirectory` (default `Path.Combine(Path.GetTempPath(), "csv2parquet")`; empty or whitespace means the default), `MaxConcurrentConversions`=2, `DuckDbThreads`=4, `DuckDbMemoryLimit`="4GB", `PreserveRowOrder`=true, `MaxUploadBytes`=null (`long?`).
- [x] 3.2 `appsettings.json`: a `Conversion` section listing every setting with its default (`"WorkDirectory": ""`, `"MaxUploadBytes": null`).
- [x] 3.3 `Program.cs`: `builder.Services.Configure<ConversionOptions>(builder.Configuration.GetSection("Conversion"))` and `builder.Services.AddSingleton<ParquetConverter>()`.
- [x] 3.4 `Program.cs` startup (after `Build()`): create `WorkDirectory` and resolve it to a full path. Fail fast with a clear message if `new DriveInfo(Path.GetPathRoot(fullPath)!)` fails (not a local drive) or if a probe file can't be created and deleted (not writable).
- [x] 3.5 `Program.cs` startup: delete files and folders in `WorkDirectory` whose last write time is older than 24 hours.
- [x] 3.6 `ParquetConverter.cs` (singleton): owns `SemaphoreSlim(MaxConcurrentConversions)`. `ConvertAsync(csvPath, parquetPath, ct)` does `await WaitAsync(ct)` with release in `finally`, opens a new `DuckDBConnection("Data Source=:memory:")` per call, and runs the locked SQL (threads, memory_limit, temp_directory, preserve_insertion_order, extension autoinstall/autoload off, `COPY … (FORMAT parquet, COMPRESSION zstd)`) with synchronous `ExecuteNonQuery()` and no `Task.Run`. Every interpolated string escapes `'` as `''`, and booleans are written in lowercase.
- [x] 3.7 `ParquetConverter.cs`: the spill folder `{WorkDirectory}/{id}.tmp` is unique to each conversion (derived from the Parquet file name) and deleted afterwards if it exists.
- [x] 3.8 `dotnet build` gives 0 warnings and 0 errors.

### Phase 4: Endpoint (`ConvertEndpoint.cs`)
- [x] 4.1 `MapConvertEndpoint(this IEndpointRouteBuilder app)` maps `POST /api/convert`. Handler parameters: `HttpContext http`, `Stream body`, and `[FromServices]` `ParquetConverter`, `IOptions<ConversionOptions>` and `ILoggerFactory`. `id = Guid.NewGuid().ToString("N")`, `csvPath = {WorkDirectory}/{id}.csv`, `parquetPath = {WorkDirectory}/{id}.parquet`. Call `app.MapConvertEndpoint()` in `Program.cs`.
- [x] 4.2 Step 1: lift the body size limit per request through `IHttpMaxRequestBodySizeFeature` (null-checked, `IsReadOnly: false`) to `MaxUploadBytes`, before the body is touched. Global server limits stay unchanged.
- [x] 4.3 Step 2: disk pre-check. If `ContentLength` has a value and free space on the WorkDirectory drive is less than 2 × `ContentLength`, return 507 ProblemDetails without reading the body.
- [x] 4.4 Step 3: stream the body to `csvPath` using `FileStreamOptions` (`CreateNew`, `Write`, `Asynchronous`, `PreallocationSize = ContentLength ?? 0`) and `CopyToAsync(csv, 1 MB, http.RequestAborted)`. The file is closed before conversion.
- [x] 4.5 Step 4: zero bytes written returns 400 ProblemDetails.
- [x] 4.6 Step 5: `await converter.ConvertAsync(...)`. A `DuckDBException` whose message starts with `Out of Memory Error` or `IO Error` returns 500 ProblemDetails; any other returns 422 ProblemDetails with the DuckDB message as `detail`. On any failure the partial Parquet is deleted.
- [x] 4.7 Step 6: return `Results.File(...)` over a `FileStream` (`Open`, `Read`, `FileShare.Read`, `Asynchronous | SequentialScan | DeleteOnClose`) with content type `application/vnd.apache.parquet` and file name `converted.parquet`. The stream is seekable, so `Content-Length` is sent. No compression or buffering middleware.
- [x] 4.8 Steps 7–8: `finally` always deletes the CSV. When `http.RequestAborted` is cancelled, clean up and return quietly with no error log. `BadHttpRequestException` propagates. No catch-all into 500. A partially uploaded file is never converted (conversion only runs after `CopyToAsync` completes).
- [x] 4.9 Step 9: one log line per conversion with CSV bytes, Parquet bytes, upload ms and conversion ms.
- [x] 4.10 `dotnet build` gives 0 warnings and 0 errors. Smoke test: `dotnet run`, `curl -T small.csv` returns a Parquet body starting with `PAR1`; then stop the app. The scratch CSV lives outside the repo.

### Phase 5: Hosting
- [x] 5.1 `Program.cs`: `WebApplication.CreateBuilder(new WebApplicationOptions { Args = args, ContentRootPath = WindowsServiceHelpers.IsWindowsService() ? AppContext.BaseDirectory : default })` and `builder.Host.UseWindowsService()`.
- [x] 5.2 `web.config`: the standard ANCM V2 handler (`AspNetCoreModuleV2`, `hostingModel="inprocess"`), `requestFiltering/requestLimits maxAllowedContentLength="4294967295"`, and `<urlCompression doStaticCompression="false" doDynamicCompression="false" />`. No `httpRuntime`/`maxRequestLength`.
- [x] 5.3 Verify with `dotnet publish -o <scratch dir>` that `web.config` is in the publish output with `inprocess`, `requestLimits` and `urlCompression` intact. The output goes outside the repo.
- [x] 5.4 `dotnet build` gives 0 warnings and 0 errors.

### Phase 6: Swagger / OpenAPI
- [x] 6.1 `builder.Services.AddOpenApi()` and `app.MapOpenApi()` serve `/openapi/v1.json`.
- [x] 6.2 `app.UseSwaggerUI(o => o.SwaggerEndpoint("/openapi/v1.json", "CSV to Parquet v1"))` in all environments, served at `/swagger`.
- [x] 6.3 Replace the template's Hello World with `MapGet("/")` redirecting to `/swagger`, with `.ExcludeFromDescription()`.
- [x] 6.4 Endpoint metadata: `.WithName("ConvertCsvToParquet")`, a summary, and a description (raw CSV, not multipart, with a curl example); `.Accepts<Stream>("text/csv")`; `.Produces<Stream>(200, "application/vnd.apache.parquet")`; `.ProducesProblem(...)` for 400, 413, 422, 500 and 507.
- [x] 6.5 Verify `/openapi/v1.json`: the request body is `text/csv` with a `{type: string, format: binary}` schema, and the 200 response is `application/vnd.apache.parquet`. If not, add an operation transformer; if 3.1 still can't express it, set `OpenApiVersion = OpenApi3_0` (pre-approved).
- [x] 6.6 `dotnet build` gives 0 warnings and 0 errors. Check `/swagger/index.html` manually with `dotnet run` + curl, then stop the app.

### Phase 7: Tests
- [x] 7.1 Delete the template's `UnitTest1.cs`. Create `TestApp.cs`: a `WebApplicationFactory<Program>` subclass that gives each instance its own unique temp `WorkDirectory` and optional `MaxUploadBytes`, deletes it on dispose, and has DuckDB helpers to read back Parquet row counts and column types.
- [x] 7.2 T1 (in-memory): a 5-row CSV with int, decimal, date and text columns returns 200 with content type `application/vnd.apache.parquet`, and the body starts and ends with `PAR1`. Saved and read back with DuckDB: 5 rows, and the int/decimal/date columns are not VARCHAR.
- [x] 7.3 T2 (in-memory): empty body returns 400.
- [x] 7.4 T3 (in-memory): a CSV DuckDB rejects (invalid UTF-8 bytes) returns 422 ProblemDetails. First confirm directly in DuckDB that this input really fails.
- [x] 7.5 T4 (in-memory): `/openapi/v1.json` returns 200, contains `/api/convert`, and the request body is `text/csv` with a binary string schema; `/swagger/index.html` returns 200.
- [x] 7.6 T5 (in-memory): after each of T1–T3, `WorkDirectory` is empty (polled for up to 2 s).
- [x] 7.7 T6 (Kestrel, LargeFile): generate a ~100 MB CSV in a temp folder and upload it with `StreamContent` over a `FileStream`, using `ResponseHeadersRead` and streaming the response to a file. Expect 200, a `Content-Length` header, a valid Parquet file and a matching row count (via DuckDB). Delete both files.
- [x] 7.8 T7 (Kestrel, LargeFile): with `MaxUploadBytes` = 1 MB, uploading 2 MB with `ExpectContinue = true` returns 413.
- [x] 7.9 `dotnet build` gives 0 warnings and 0 errors; `dotnet test` passes everything, including `Category=LargeFile`.

### Phase 8: README and final check
- [x] 8.1 README: how to run locally, and the config table.
- [x] 8.2 README: the curl `-T` example, with a note that `-T` streams while `--data-binary @file` loads everything into memory first.
- [x] 8.3 README: the C# `HttpClient` example (`StreamContent` over `File.OpenRead`, `Timeout = Timeout.InfiniteTimeSpan`, `SendAsync(..., ResponseHeadersRead)` then `CopyToAsync` into a `FileStream`), with a note that without `ResponseHeadersRead` HttpClient buffers the whole Parquet in memory and fails above 2 GB.
- [x] 8.4 README deployment notes: WorkDirectory on a local data drive with ~2 × the largest CSV free, write access for the account, AV exclusion; the IIS ~4 GB cap, with Windows Service mode and an `sc.exe create` example for larger files; reverse proxy/LB body limits and an idle timeout longer than the slowest conversion; Swagger UI is for small files only.
- [x] 8.5 Final check: `dotnet build` 0/0, `dotnet test` all green, grep of `src/` for forbidden APIs, `/openapi/v1.json` content check, every S1–S11 ticked, no app left running.
- [x] 8.6 Final summary: files created, package versions, deviations, how to run, and ✅/❌ for each acceptance item.

### Phase 9: Parallel requests, configurable limits and speed
- [x] 9.1 `ConversionOptions`: add `MaxConcurrentRequests` (0 = no limit), `MaxQueuedRequests` (`int?`, null = as many as are admitted), `ConversionQueueTimeoutSeconds` (0 = no timeout), `ParquetCompression` ("zstd"), `ParquetCompressionLevel` (`int?`), `ParquetRowGroupSize` (`long?`). Change `MaxConcurrentConversions` and `DuckDbThreads` to 0 = auto and `DuckDbMemoryLimit` to "" = DuckDB's default, exposed as `Effective*` properties.
- [x] 9.2 `ConversionOptions.Validate()`: fail fast on a codec outside the allow-list (the value is interpolated into SQL), a compression level outside 1-22 or set for a codec other than zstd (DuckDB rejects that combination), a row group size below 1, a memory limit that is not a size, and a negative limit. Called from the converter's constructor, which is resolved at startup.
- [x] 9.3 `ParquetConverter`: hold one open `DuckDBConnection("Data Source=:memory:")` for the life of the app, apply the DuckDB globals to it once, and give each conversion a `Duplicate()` of it. Measured: ~15 ms to create an instance versus ~0.3 ms to duplicate a connection.
- [x] 9.4 `ParquetConverter`: run `ExecuteNonQuery` on a dedicated `Thread` and register `DuckDBCommand.Cancel()` on the cancellation token, so a conversion never occupies a thread-pool thread and an abandoned one releases its slot. `ConvertAsync` returns `ConversionTimings?`, null meaning no slot came free within `ConversionQueueTimeoutSeconds`.
- [x] 9.5 `ParquetConverter`: one spill folder per process, `{WorkDirectory}/spill-{ProcessId}`, because DuckDB manages its own spill files and an overlapped IIS recycle runs two processes against one WorkDirectory.
- [x] 9.6 `Program.cs`: `AddRateLimiter` with a concurrency policy built from `IOptions<ConversionOptions>` (no limiter when `MaxConcurrentRequests` is 0), `OnRejected` writing 503 ProblemDetails with `Retry-After`, `app.UseRateLimiter()`, and `.RequireRateLimiting` on the endpoint. Resolve `ParquetConverter` after the WorkDirectory check so DuckDB is warm and bad settings fail at startup.
- [x] 9.7 `ConvertEndpoint`: return 503 with `Retry-After` when `ConvertAsync` returns null, add `.ProducesProblem(503)`, log the queue wait alongside the upload and conversion times, and open the upload `FileStream` with `BufferSize = 0` so the 1 MB copy buffer is not copied again.
- [x] 9.8 `appsettings.json`: every new setting with its default.
- [x] 9.9 Tests T8-T13 (section 7). `TestApp` gains a `Settings` dictionary for `Conversion:*` overrides and treats the per-process spill folder as part of an empty WorkDirectory.
- [x] 9.10 `dotnet build` 0 warnings and 0 errors; `dotnet test` all green including `Category=LargeFile`; the parallel tests repeated 5 times with no flakes.
- [x] 9.11 README: the new settings, the three limits, what to tune in what order, and the before/after measurements.

## 6. Large-file safeguards

- [x] S1: Body size limit lifted per request via `IHttpMaxRequestBodySizeFeature`, before the body is read (Phase 4). Implemented by task 4.2.
- [x] S2: Disk pre-check returns 507 before reading when free space < 2 × `Content-Length` (Phase 4). Implemented by task 4.3.
- [x] S3: Upload streamed to disk with preallocation; never buffered in memory (Phase 4). Implemented by task 4.4.
- [x] S4: Partial or aborted uploads are never converted; temp files are always cleaned up (Phase 4). Implemented by tasks 4.8, 4.6 and 4.7 (spill folder: 3.7).
- [x] S5: DuckDB bounded by `threads`, `memory_limit`, a per-conversion spill folder and the concurrency semaphore (Phase 3). Implemented by tasks 3.6 and 3.7.
- [x] S6: Parquet streamed from disk with `Content-Length`, deleted on close, with no compression or buffering (Phase 4). Implemented by task 4.7.
- [x] S7: IIS config: `maxAllowedContentLength` at its maximum, in-process hosting, compression off (Phase 5). Implemented by tasks 5.2 and 5.3.
- [x] S8: Windows Service mode for files over ~4 GB (Phase 5). Implemented by task 5.1.
- [x] S9: WorkDirectory validated at startup; stale files removed (Phase 3). Implemented by tasks 3.4 and 3.5.
- [x] S10: README client examples stream in both directions and raise client timeouts (Phase 8). Implemented by tasks 8.2 and 8.3.
- [x] S11: Proven by tests T6 (100 MB over real Kestrel) and T7 (413 wiring) (Phase 7). Implemented by tasks 7.7 and 7.8.

## 7. Tests

Test hosts:
- The in-memory TestServer, for fast tests.
- Real Kestrel via `WebApplicationFactory.UseKestrel()` + `StartServer()`, for large-file tests. Call `UseKestrel()` before any `CreateClient()`/`StartServer()`. The in-memory server does not enforce Kestrel's body size limit, so only real Kestrel proves S1.

Use a separate factory instance per configuration, each with its own unique temp `WorkDirectory`. Keep tests that inspect `WorkDirectory` in one class so they run sequentially.

In-memory:
- T1: a 5-row CSV with int, decimal, date and text columns → 200, content type `application/vnd.apache.parquet`, body starts and ends with `PAR1`. Save it and read it back with DuckDB: 5 rows, and the int/decimal/date columns are typed (not VARCHAR).
- T2: empty body → 400.
- T3: a CSV DuckDB rejects (e.g. invalid UTF-8 bytes) → 422 ProblemDetails. Confirm the input really fails in DuckDB; don't assume.
- T4: `/openapi/v1.json` → 200, contains `/api/convert`, and the request body is `text/csv` with a binary string schema; `/swagger/index.html` → 200.
- T5: after each of T1–T3, `WorkDirectory` is empty. Poll for up to 2 s, because the Parquet is deleted when the response stream closes.

Real Kestrel, marked `[Trait("Category", "LargeFile")]`:
- T6: generate a ~100 MB CSV in a temp folder (well above the 30 MB default limit).
  - Upload it with `StreamContent` over a `FileStream`.
  - Send with `HttpCompletionOption.ResponseHeadersRead` and stream the response into a file.
  - Expect 200, a `Content-Length` header, a valid Parquet file, and a matching row count (read with DuckDB).
  - Delete both files afterwards.
- T7: with `MaxUploadBytes` = 1 MB, upload 2 MB with `ExpectContinue = true` → 413.

## 8. Acceptance checklist

- [x] `dotnet build`: 0 warnings, 0 errors
- [x] `dotnet test`: all pass, including the LargeFile tests
- [x] A grep of `src/` finds none of: `IFormFile`, `ReadFormAsync`, `EnableBuffering`, `MemoryStream`, `ReadAllBytes`
- [x] `/openapi/v1.json`: `text/csv` binary request body; `application/vnd.apache.parquet` 200 response
- [x] Every large-file safeguard (S1–S11) is ticked in plan.md with its implementing task
- [x] README covers everything listed in Phase 8
- [x] plan.md: all tasks ticked; any deviations logged and approved
- [x] Final summary: files created, package versions, deviations, how to run

## 9. Deviations log

| # | Change | Reason | Approved |
|---|---|---|---|
| 1 | Task 3.6's "a new `DuckDBConnection` per call" becomes one shared in-memory instance with a duplicated connection per conversion | Creating an instance costs ~15 ms against ~0.3 ms to duplicate, and one instance lets DuckDB's scheduler share `DuckDbThreads` across conversions instead of each conversion guessing its share. Measured on 4 cores: one conversion 766 ms shared against 853 ms with its own instance; 4 at once 1142 ms against 1427 ms with 4 threads each | Requested: "make it as fast as possible with parallel requests support" |
| 2 | Task 3.6's "synchronous `ExecuteNonQuery()` and no `Task.Run`" becomes a dedicated `Thread` per conversion | DuckDB has no async API, so `ExecuteNonQuery` blocks for the whole conversion. On a thread-pool thread that starves the pool the other parallel requests need, and the pool only grows about one thread per second. Measured with 8 conversions in flight: `GET /openapi/v1.json` 940 ms before, 43 ms after | As above |
| 3 | `DuckDbThreads` and `DuckDbMemoryLimit` become totals for the service rather than per conversion, and `DuckDbMemoryLimit` defaults to DuckDB's own (~80% of RAM) instead of 4GB | They are DuckDB global settings, so one shared instance can only have one value. A total is also the honest number to reason about: the old default allowed `MaxConcurrentConversions x 4GB`. Set an explicit total when the service shares a machine | As above |
| 4 | `MaxConcurrentConversions` and `DuckDbThreads` default to CPU-core counts instead of 2 and 4 | The old fixed defaults used at most 8 threads however large the machine. Measured at defaults, 8 conversions in parallel: 2676 ms before, 2409 ms after | As above |
| 5 | Per-conversion spill folder `{id}.tmp` (task 3.7) becomes one per process, `spill-{ProcessId}` | A single DuckDB instance manages its own spill files, so per-conversion folders no longer apply. Keyed by process id rather than a fixed name because an overlapped IIS app-pool recycle runs two worker processes against one WorkDirectory | As above |
| 6 | Row order is **not** reordered by default (`PreserveRowOrder` stays true) | `PreserveRowOrder: false` is the single biggest speed-up (about 20% off one conversion) but changes the output rows' order, which callers may rely on. Left as an opt-in and documented first in the README's tuning list | Judgement call; flagged in the summary |
