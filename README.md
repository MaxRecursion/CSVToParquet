# CSV to Parquet

A small internal service that converts CSV files to Parquet. Large files (multi-GB) are the main use case.

- One endpoint, `POST /api/convert`. The request body is the raw CSV (`Content-Type: text/csv`, not multipart) and the response body is the Parquet file.
- The upload is streamed to a temp file on disk, and [DuckDB](https://duckdb.org) converts it with a single `COPY` statement. DuckDB parses in parallel, detects column types, uses zstd compression and spills to disk instead of running out of memory.
- The Parquet is streamed back from disk with `Content-Length` and deleted when the response ends. Neither file is ever held in memory.
- The conversion runs synchronously inside the request. At most `MaxConcurrentConversions` conversions run at once; the rest wait.

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
| MaxConcurrentConversions | 2 | DuckDB conversions at once |
| DuckDbThreads | 4 | Per conversion; keep threads × concurrency ≈ CPU cores |
| DuckDbMemoryLimit | "4GB" | Per conversion; limit × concurrency must fit in RAM |
| PreserveRowOrder | true | Set to false if a huge file runs out of memory |
| MaxUploadBytes | null | null = no limit |

An empty `WorkDirectory` means the default temp folder. At startup the service creates the folder, fails fast if it isn't on a local drive or isn't writable, and deletes leftovers older than 24 hours.

## Responses

| Status | When |
|---|---|
| 200 | The Parquet file (`application/vnd.apache.parquet`, with `Content-Length`) |
| 400 | Empty body, or a malformed or truncated request |
| 413 | Upload larger than `MaxUploadBytes` (under IIS, uploads over ~4 GB are rejected by IIS itself with 404.13) |
| 415 | A `Content-Type` other than `text/csv` |
| 422 | DuckDB can't read the CSV, for example invalid UTF-8, or a column whose type changes after the first ~20,000 rows DuckDB samples to detect types; `detail` holds DuckDB's message |
| 500 | Server-side DuckDB failure (out of memory, I/O error) |
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

## Deployment

The same build runs under IIS (in-process) or as a Windows Service running Kestrel. Publish it with:

```bash
dotnet publish src/CsvToParquet.Api -c Release -r win-x64 --self-contained false -o publish
```

### WorkDirectory

- Put it on a **local data drive** with free space of about **2 × the largest CSV**, not a small C: drive. The disk holds the uploaded CSV, the Parquet and DuckDB's spill files at the same time. Set an absolute path, for example `"WorkDirectory": "D:\\csv2parquet"`.
- The **app-pool or service account** needs write access to it (for example `IIS AppPool\CsvToParquet` for IIS).
- Ideally exclude the folder from **real-time antivirus scanning**. Scanning multi-GB temp files slows every conversion and can lock files.

### IIS: uploads up to ~4 GB

- Install the ASP.NET Core 10 Hosting Bundle. Use an app pool with *No Managed Code* and 64-bit, because DuckDB ships no 32-bit Windows binary.
- The included `web.config` uses in-process hosting (out-of-process adds a 2-minute request timeout) and sets `maxAllowedContentLength="4294967295"`. Compression is off because Parquet is already compressed.
- **IIS caps uploads at ~4 GB** (`maxAllowedContentLength` is a 32-bit value); request filtering rejects larger uploads with 404.13 before the app sees them. For larger files use Windows Service mode.

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
- an **idle/read timeout longer than the slowest conversion**, because no bytes flow while DuckDB converts. A 60-second default drops large conversions.

### Swagger UI

Swagger UI is for **small test files** only: the browser holds the whole response in memory. Use curl or `HttpClient` for real files.
