using Parquet;
using Parquet.Query.Dynamic;
using Parquet.Serialization;

namespace Parquet.Query.Tests;

public sealed class DynamicParquetQueryTests : IAsyncLifetime
{
    private readonly string _tempDirectory = Path.Combine(Path.GetTempPath(), "Parquet.Query.Tests.Dynamic", Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task ExecuteAsync_returns_all_rows_when_no_predicates()
    {
        var filePath = await WriteTestFileAsync("all-rows.parquet");

        using var reader = await ParquetReader.CreateAsync(filePath);
        var query = DynamicParquetQuery.FromReader(reader, filePath);
        var result = await query.ExecuteAsync();

        Assert.Equal(4, result.TotalMatchedRows);
        Assert.Equal(4, result.Rows.Count);
        Assert.Equal(1, result.Rows[0]["Id"]);
        Assert.Equal("alpha", result.Rows[0]["Name"]);
    }

    [Fact]
    public async Task ExecuteAsync_equality_predicate_filters_rows()
    {
        var filePath = await WriteTestFileAsync("eq-filter.parquet");

        using var reader = await ParquetReader.CreateAsync(filePath);
        var query = DynamicParquetQuery.FromReader(reader, filePath)
            .Where(new DynamicPredicate("Country", "==", "US"));
        var result = await query.ExecuteAsync();

        Assert.Equal(2, result.TotalMatchedRows);
        Assert.All(result.Rows, row => Assert.Equal("US", row["Country"]));
    }

    [Fact]
    public async Task ExecuteAsync_range_predicate_prunes_row_groups()
    {
        var filePath = await WriteTestFileAsync("range-filter.parquet");

        using var reader = await ParquetReader.CreateAsync(filePath);
        var query = DynamicParquetQuery.FromReader(reader, filePath)
            .Where(new DynamicPredicate("Age", ">=", "30"));
        var result = await query.ExecuteAsync();

        Assert.Equal(1, result.TotalMatchedRows);
        Assert.Equal("delta", result.Rows[0]["Name"]);

        // Verify row group pruning happened
        var plan = result.Plan;
        Assert.True(plan.RowGroups.Any(rg => !rg.ShouldRead),
            "Expected at least one row group to be pruned by statistics.");
    }

    [Fact]
    public async Task ExecuteAsync_between_predicate_works()
    {
        var filePath = await WriteTestFileAsync("between-filter.parquet");

        using var reader = await ParquetReader.CreateAsync(filePath);
        var query = DynamicParquetQuery.FromReader(reader, filePath)
            .Where(new DynamicPredicate("Age", "between", "15", "25"));
        var result = await query.ExecuteAsync();

        // Age 15 (gamma) and Age 20 (bravo) both fall within [15..25]
        Assert.Equal(2, result.TotalMatchedRows);
        var names = result.Rows.Select(r => (string)r["Name"]!).OrderBy(n => n).ToArray();
        Assert.Equal(new[] { "bravo", "gamma" }, names);
    }

    [Fact]
    public async Task ExecuteAsync_startswith_predicate_works()
    {
        var filePath = await WriteTestFileAsync("startswith-filter.parquet");

        using var reader = await ParquetReader.CreateAsync(filePath);
        var query = DynamicParquetQuery.FromReader(reader, filePath)
            .Where(new DynamicPredicate("Name", "startsWith", "al"));
        var result = await query.ExecuteAsync();

        Assert.Equal(1, result.TotalMatchedRows);
        Assert.Equal("alpha", result.Rows[0]["Name"]);
    }

    [Fact]
    public async Task ExecuteAsync_paging_works()
    {
        var filePath = await WriteTestFileAsync("paging.parquet");

        using var reader = await ParquetReader.CreateAsync(filePath);
        var query = DynamicParquetQuery.FromReader(reader, filePath);
        var result = await query.ExecuteAsync(offset: 1, limit: 2);

        Assert.Equal(4, result.TotalMatchedRows);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal(2, result.Rows[0]["Id"]);
        Assert.Equal(3, result.Rows[1]["Id"]);
    }

    [Fact]
    public async Task PlanAsync_returns_row_group_decisions()
    {
        var filePath = await WriteTestFileAsync("plan.parquet");

        using var reader = await ParquetReader.CreateAsync(filePath);
        var query = DynamicParquetQuery.FromReader(reader, filePath)
            .Where(new DynamicPredicate("Age", ">", "25"));
        var plan = await query.PlanAsync();

        Assert.True(plan.RowGroups.Count > 0);
        Assert.True(plan.RowGroups.Any(rg => !rg.ShouldRead),
            "Expected statistics-based row group pruning.");
    }

    [Fact]
    public async Task CountAsync_returns_correct_count()
    {
        var filePath = await WriteTestFileAsync("count.parquet");

        using var reader = await ParquetReader.CreateAsync(filePath);
        var noFilterCount = await DynamicParquetQuery.FromReader(reader, filePath).CountAsync();
        var filteredCount = await DynamicParquetQuery.FromReader(reader, filePath)
            .Where(new DynamicPredicate("Country", "==", "DE"))
            .CountAsync();

        Assert.Equal(4, noFilterCount);
        Assert.Equal(2, filteredCount);
    }

    [Fact]
    public async Task ToAsyncEnumerable_streams_results()
    {
        var filePath = await WriteTestFileAsync("stream.parquet");

        using var reader = await ParquetReader.CreateAsync(filePath);
        var query = DynamicParquetQuery.FromReader(reader, filePath)
            .Where(new DynamicPredicate("Country", "==", "US"));

        var rows = new List<IReadOnlyDictionary<string, object?>>();
        await foreach (var row in query.ToAsyncEnumerable())
            rows.Add(row);

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal("US", row["Country"]));
    }

    [Fact]
    public async Task FromReader_convenience_on_ParquetQuery_works()
    {
        var filePath = await WriteTestFileAsync("convenience.parquet");

        using var reader = await ParquetReader.CreateAsync(filePath);
        var query = ParquetQuery.FromReader(reader, filePath)
            .Where(new DynamicPredicate("Id", "==", "1"));
        var result = await query.ExecuteAsync();

        Assert.Equal(1, result.TotalMatchedRows);
        Assert.Equal("alpha", result.Rows[0]["Name"]);
    }

    private Task<string> WriteTestFileAsync(string fileName)
    {
        var filePath = Path.Combine(_tempDirectory, fileName);
        var rows = new[]
        {
            new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
            new TestRow { Id = 2, Country = "US", Name = "bravo", Age = 20 },
            new TestRow { Id = 3, Country = "DE", Name = "gamma", Age = 15 },
            new TestRow { Id = 4, Country = "US", Name = "delta", Age = 30 },
        };

        var serializerOptions = new ParquetSerializerOptions
        {
            RowGroupSize = 2,
        };

        return ParquetSerializer.SerializeAsync(rows, filePath, serializerOptions)
            .ContinueWith(_ => filePath);
    }

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_tempDirectory);
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        if (Directory.Exists(_tempDirectory))
            Directory.Delete(_tempDirectory, recursive: true);
        return Task.CompletedTask;
    }
}
