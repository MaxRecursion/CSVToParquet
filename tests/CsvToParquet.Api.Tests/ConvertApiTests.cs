using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using DuckDB.NET.Data;
using Microsoft.AspNetCore.Mvc;

namespace CsvToParquet.Api.Tests;

/// <summary>In-memory TestServer tests. They inspect the WorkDirectory, so they live in one class and run sequentially.</summary>
public sealed class ConvertApiTests(TestApp app) : IClassFixture<TestApp>
{
    private static readonly byte[] ParquetMagic = "PAR1"u8.ToArray();

    [Fact]
    public async Task T1_ValidCsv_ReturnsTypedParquet()
    {
        const string csv = """
            id,price,day,name
            1,10.50,2024-01-01,alpha
            2,20.25,2024-01-02,beta
            3,30.00,2024-01-03,gamma
            4,40.75,2024-01-04,delta
            5,50.10,2024-01-05,epsilon

            """;
        using var client = app.CreateClient();

        using var response = await client.PostAsync("/api/convert", CsvContent(Encoding.UTF8.GetBytes(csv)));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("application/vnd.apache.parquet", response.Content.Headers.ContentType?.MediaType);
        var parquet = await response.Content.ReadAsByteArrayAsync();
        Assert.Equal(ParquetMagic, parquet[..4]);
        Assert.Equal(ParquetMagic, parquet[^4..]);

        var parquetPath = Path.Combine(Path.GetTempPath(), $"t1-{Guid.NewGuid():N}.parquet");
        try
        {
            await File.WriteAllBytesAsync(parquetPath, parquet);
            Assert.Equal(5, TestApp.CountParquetRows(parquetPath));
            var types = TestApp.ParquetColumnTypes(parquetPath);
            Assert.NotEqual("VARCHAR", types["id"]);
            Assert.NotEqual("VARCHAR", types["price"]);
            Assert.NotEqual("VARCHAR", types["day"]);
            Assert.Equal("VARCHAR", types["name"]);
        }
        finally
        {
            File.Delete(parquetPath);
        }

        Assert.Empty(await app.WaitForEmptyWorkDirectoryAsync()); // T5
    }

    [Fact]
    public async Task T2_EmptyBody_Returns400()
    {
        using var client = app.CreateClient();

        using var response = await client.PostAsync("/api/convert", CsvContent([]));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(await app.WaitForEmptyWorkDirectoryAsync()); // T5
    }

    [Fact]
    public async Task T3_CsvRejectedByDuckDb_Returns422ProblemDetails()
    {
        byte[] invalidUtf8 = [.. "id,name\n1,ok\n2,"u8, 0xFF, 0xFE, 0xFD, .. " bad\n3,x\n"u8];

        // Confirm DuckDB really rejects this input rather than assuming it.
        var csvPath = Path.Combine(Path.GetTempPath(), $"t3-{Guid.NewGuid():N}.csv");
        try
        {
            await File.WriteAllBytesAsync(csvPath, invalidUtf8);
            Assert.Throws<DuckDBException>(() => TestApp.CountCsvRows(csvPath));
        }
        finally
        {
            File.Delete(csvPath);
        }

        using var client = app.CreateClient();

        using var response = await client.PostAsync("/api/convert", CsvContent(invalidUtf8));

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        var problem = await response.Content.ReadFromJsonAsync<ProblemDetails>();
        Assert.Equal(422, problem?.Status);
        Assert.False(string.IsNullOrEmpty(problem?.Detail));
        Assert.Empty(await app.WaitForEmptyWorkDirectoryAsync()); // T5
    }

    [Fact]
    public async Task T4_OpenApiDocumentAndSwaggerUi()
    {
        using var client = app.CreateClient();

        using var openApi = await client.GetAsync("/openapi/v1.json");

        Assert.Equal(HttpStatusCode.OK, openApi.StatusCode);
        using var document = JsonDocument.Parse(await openApi.Content.ReadAsStringAsync());
        var root = document.RootElement;
        Assert.True(root.GetProperty("paths").TryGetProperty("/api/convert", out var path));
        var schema = path.GetProperty("post").GetProperty("requestBody").GetProperty("content").GetProperty("text/csv").GetProperty("schema");
        if (schema.TryGetProperty("$ref", out var reference))
        {
            // "#/components/schemas/Stream"
            schema = root.GetProperty("components").GetProperty("schemas").GetProperty(reference.GetString()!.Split('/')[^1]);
        }

        Assert.Equal("string", schema.GetProperty("type").GetString());
        Assert.Equal("binary", schema.GetProperty("format").GetString());

        using var swagger = await client.GetAsync("/swagger/index.html");
        Assert.Equal(HttpStatusCode.OK, swagger.StatusCode);
    }

    private static ByteArrayContent CsvContent(byte[] bytes)
    {
        var content = new ByteArrayContent(bytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("text/csv");
        return content;
    }
}
