# CSV to Parquet

A small internal service that converts CSV files to Parquet. Large files (multi-GB) are the main use case.

- One endpoint, `POST /api/convert`. The request body is the raw CSV (`Content-Type: text/csv`, not multipart) and the response body is the Parquet file.
- The upload is streamed to a temp file on disk, and [DuckDB](https://duckdb.org) converts it with a single `COPY` statement. DuckDB parses in parallel, detects column types, uses zstd compression and spills to disk instead of running out of memory.
- The Parquet is streamed back from disk with `Content-Length` and deleted when the response ends. Neither file is ever held in memory.
- The conversion runs synchronously inside the request, on a thread of its own so that it never blocks the thread pool the other requests need. Requests are served in parallel: `MaxConcurrentRequests` caps how many are admitted and `MaxConcurrentConversions` caps how many convert at once; the rest queue.
- One warm in-memory DuckDB instance is shared by every conversion, so no request pays for DuckDB's start-up and DuckDB's own scheduler spreads `DuckDbThreads` worker threads across whatever is running.
- When a client disconnects, its conversion is interrupted within milliseconds and its slot goes to a request that is still waiting.

## Run locally

Requires the .NET 10 SDK (`global.json` pins 10.0.302 and rolls forward within 10.0).

```bash
dotnet run --project src/CsvToParquet.Api
```

This serves `http://localhost:5100` (from `Properties/launchSettings.json`):

- Swagger UI: `http://localhost:5100/swagger` (`/` redirects there)
- OpenAPI document: `http://localhost:5100/openapi/v1.json`

Run the tests. The `LargeFile` category pushes a ~100 MB CSV through real Kestrel.

```bash
dotnet test
```

```bash
dotnet test --filter Category=LargeFile
```

## Configuration

Settings live in the `Conversion` section of `appsettings.json`. Override them as usual, for example with the environment variable `Conversion__WorkDirectory=D:\csv2parquet` or the command-line argument `--Conversion:MaxConcurrentConversions=4`.

| Setting | Default | Meaning |
|---|---|---|
| WorkDirectory | `Path.Combine(Path.GetTempPath(), "csv2parquet")` | Local disk for temp CSV, Parquet and spill files |
| MaxConcurrentRequests | 0 | Requests admitted at once; 0 = no limit |
| MaxQueuedRequests | null | Requests queued once the limit is reached, then 503; null = as many as are admitted |
| MaxConcurrentConversions | 0 | DuckDB conversions at once; 0 = one per CPU core, at least 2 and at most 8 |
| ConversionQueueTimeoutSeconds | 0 | Seconds a request waits for a conversion slot before 503; 0 = as long as the client waits |
| DuckDbThreads | 0 | Worker threads **shared by every conversion**; 0 = one per CPU core |
| DuckDbMemoryLimit | "" | Memory **shared by every conversion**; empty = DuckDB's own default, about 80% of RAM |
| PreserveRowOrder | true | false reorders rows and is the single biggest speed-up |
| ParquetCompression | "zstd" | `uncompressed`, `snappy`, `gzip`, `zstd`, `brotli`, `lz4` or `lz4_raw` |
| ParquetCompressionLevel | null | zstd only, 1 (fastest) to 22; null = DuckDB's default of 3 |
| ParquetRowGroupSize | null | Rows per row group; null = DuckDB's default of 122880 |
| MaxUploadBytes | null | null = no limit |

An empty `WorkDirectory` means the default temp folder. At startup the service creates the folder, fails fast if it isn't on a local drive or isn't writable, and deletes leftovers older than 24 hours. A setting the service cannot use — an unknown codec, a memory limit that isn't a size, a negative limit — fails at startup with a message naming it, not on the first request.

`MaxConcurrentRequests`, `MaxQueuedRequests`, `DuckDbThreads`, `DuckDbMemoryLimit`, `PreserveRowOrder` and the Parquet settings are read once at startup, so changing them needs a restart.

## Parallelism and speed

Three separate limits, from the outside in:

1. **`MaxConcurrentRequests`** admits requests. Beyond it, `MaxQueuedRequests` wait and the rest get 503 with `Retry-After` — before their upload touches the disk. Leave it at 0 unless you need to protect the WorkDirectory drive or the network from bursts.
2. **`MaxConcurrentConversions`** caps how many DuckDB `COPY` statements run at once. Uploads and downloads are never capped, so a queued request keeps streaming its body while the ones ahead of it convert.
3. **`DuckDbThreads`** and **`DuckDbMemoryLimit`** are totals for the whole service, because all conversions share one DuckDB instance. Leave `DuckDbThreads` at one per core: DuckDB's scheduler already gives a lone conversion every thread and shares them out when several run. More threads than cores measurably slowed conversions down.

Tuning, in the order worth trying:

- **`PreserveRowOrder: false`** — the biggest single win, about 20% off one conversion and 10% off a batch, because DuckDB no longer buffers to keep input order. Only safe if row order in the Parquet doesn't matter to whoever reads it.
- **`ParquetCompression: "snappy"`** — about as much again, at the cost of a noticeably larger file. `zstd` with `ParquetCompressionLevel: 1` is a middle ground.
- **`MaxConcurrentConversions`** — raise it only if conversions are waiting on disk rather than CPU. Above one per core it stops helping.
- **`DuckDbMemoryLimit`** — set an explicit total when the service shares a machine. Below what a conversion needs, DuckDB spills to the WorkDirectory drive instead of failing, so a low limit costs disk I/O rather than errors.

Measured on a 4-core, 16 GB Linux container with a 50 MB CSV, comparing against the previous behaviour (a fresh DuckDB instance per request, `COPY` on a thread-pool thread, 2 conversions at once):

| | Before | After |
|---|---|---|
| First conversion after startup | 618 ms | 530 ms |
| One conversion, server idle | 441 ms | 372 ms |
| 8 conversions in parallel, default settings | 2676 ms | 2409 ms |
| `GET /openapi/v1.json` during 8 conversions | 940 ms | 43 ms |
| 50-row conversion during 8 conversions | 487 ms | 42 ms |

The last two rows are the point: converting no longer holds thread-pool threads, so the rest of the service stays responsive while the CPU is busy. Raw throughput of 8 simultaneous conversions on 4 cores is unchanged (2511 ms vs 2589 ms, within run-to-run noise) — at that point the CPU is the limit, not the scheduling.

## Responses

| Status | When |
|---|---|
| 200 | The Parquet file (`application/vnd.apache.parquet`, with `Content-Length`) |
| 400 | Empty body, or a malformed or truncated request |
| 413 | Upload larger than `MaxUploadBytes` (under IIS, uploads over ~4 GB are rejected by IIS itself with 404.13) |
| 415 | A `Content-Type` other than `text/csv` |
| 422 | DuckDB can't read the CSV, for example invalid UTF-8, or a column whose type changes after the first ~20,000 rows DuckDB samples to detect types; `detail` holds DuckDB's message |
| 500 | Server-side DuckDB failure (out of memory, I/O error) |
| 503 | Over `MaxConcurrentRequests` + `MaxQueuedRequests`, or no conversion slot came free within `ConversionQueueTimeoutSeconds`. Carries `Retry-After` |
| 507 | Free space on the WorkDirectory drive is less than 2 × `Content-Length` |

Errors are returned as `application/problem+json`.

## Clients

A multi-GB conversion can take minutes, and no bytes flow while DuckDB converts. Clients must stream in both directions and must not time out early.

### curl

```bash
curl -T data.csv -X POST -H "Content-Type: text/csv" http://localhost:<port>/api/convert -o data.parquet
```

`-T` streams the file from disk. `--data-binary @file` loads the whole file into memory before sending. `-X POST` is needed because `-T` sends PUT otherwise. curl has no overall timeout by default, so don't add a short `--max-time`.

### C# HttpClient

```csharp
using System.Net.Http.Headers;

using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan }; // the default 100 s is far too short

await using var csv = File.OpenRead("data.csv");
using var request = new HttpRequestMessage(HttpMethod.Post, "http://localhost:5100/api/convert")
{
    Content = new StreamContent(csv) // streams from disk and sends Content-Length
};
request.Content.Headers.ContentType = new MediaTypeHeaderValue("text/csv");

using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
response.EnsureSuccessStatusCode();

await using var parquet = new FileStream("data.parquet", FileMode.Create, FileAccess.Write, FileShare.None, 1024 * 1024, useAsync: true);
await response.Content.CopyToAsync(parquet);
```

> Without `HttpCompletionOption.ResponseHeadersRead`, `HttpClient` buffers the whole Parquet in memory before `SendAsync` returns, and fails for responses above 2 GB.

Send requests in parallel to convert several files at once; the service handles the queueing. If you set `MaxConcurrentRequests`, retry a 503 after the `Retry-After` delay rather than treating it as a failure. Cancelling a request stops its conversion on the server, so abandoning work costs nothing.

## Deployment

The same build runs under IIS (in-process) or as a Windows Service running Kestrel. Publish it with:

```bash
dotnet publish src/CsvToParquet.Api -c Release -r win-x64 --self-contained false -o publish
```

### WorkDirectory

- Put it on a **local data drive** with free space of about **2 × the largest CSV × `MaxConcurrentConversions`**, not a small C: drive. The disk holds the uploaded CSV, the Parquet and DuckDB's spill files at the same time, for every conversion in flight. Set an absolute path, for example `"WorkDirectory": "D:\\csv2parquet"`.
- The **app-pool or service account** needs write access to it (for example `IIS AppPool\CsvToParquet` for IIS).
- Ideally exclude the folder from **real-time antivirus scanning**. Scanning multi-GB temp files slows every conversion and can lock files.
- Each process gets its own `spill-<pid>` folder inside it, so an overlapped app-pool recycle cannot disturb the draining process's conversions. A folder left behind by a crash is removed by the startup clean-up once it is a day old.

### IIS: uploads up to ~4 GB

- Install the ASP.NET Core 10 Hosting Bundle. Use an app pool with *No Managed Code* and 64-bit, because DuckDB ships no 32-bit Windows binary.
- The included `web.config` uses in-process hosting (out-of-process adds a 2-minute request timeout) and sets `maxAllowedContentLength="4294967295"`. Compression is off because Parquet is already compressed.
- **IIS caps uploads at ~4 GB** (`maxAllowedContentLength` is a 32-bit value); request filtering rejects larger uploads with 404.13 before the app sees them. For larger files use Windows Service mode.
- IIS may run **several worker processes** for one app pool (web gardens, overlapped recycles). Each has its own DuckDB instance, so `DuckDbThreads` and `DuckDbMemoryLimit` apply per process, not per app pool.

### Windows Service: no size cap

Kestrel runs directly with no upload cap, unless you set `MaxUploadBytes`. The content root is pinned to the app folder, so `appsettings.json` is found even though services start in System32.

```bat
sc.exe create CsvToParquet binPath= "C:\Services\CsvToParquet\CsvToParquet.Api.exe --urls http://0.0.0.0:5080" start= auto obj= "NT AUTHORITY\NetworkService"
sc.exe description CsvToParquet "Converts CSV files to Parquet"
sc.exe start CsvToParquet
```

Give the service account (here `NetworkService`) write access to the WorkDirectory, and open the port in the firewall.

### Reverse proxies and load balancers

Anything in front of the service needs:

- a **request body size limit** at least as large as the largest CSV, with request and response buffering off where possible;
- an **idle/read timeout longer than the slowest conversion**, because no bytes flow while DuckDB converts. A 60-second default drops large conversions;
- a **concurrency limit no lower than the service's own**, or it will queue requests where the service cannot see them. Prefer letting the service shed load with 503, so clients get `Retry-After` instead of a timeout.

### Swagger UI

Swagger UI is for **small test files** only: the browser holds the whole response in memory. Use curl or `HttpClient` for real files.
