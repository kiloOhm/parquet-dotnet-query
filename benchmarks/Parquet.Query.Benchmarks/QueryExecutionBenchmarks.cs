using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Parquet;
using Parquet.Query;
using Parquet.Serialization;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class QueryExecutionBenchmarks
{
    private string _filePath = string.Empty;

    [Params(20_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        var directory = Path.Combine(Path.GetTempPath(), "Parquet.Query.Benchmarks", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        _filePath = Path.Combine(directory, "people.parquet");

        var rows = Enumerable.Range(0, RowCount)
            .Select(index => new BenchmarkRow
            {
                Id = index,
                Country = index % 10 == 0 ? "DE" : "US",
                Name = $"name-{index:D5}",
                Age = index % 100,
                Score = index
            })
            .ToArray();

        var parquetOptions = new ParquetOptions
        {
            DataPageRowCountLimit = 256
        };
        parquetOptions.BloomFilterOptionsByColumn["Country"] = new ParquetOptions.BloomFilterOptions
        {
            EnableBloomFilters = true
        };

        await ParquetSerializer.SerializeAsync(
            rows,
            _filePath,
            new ParquetSerializerOptions
            {
                RowGroupSize = 4_000,
                ParquetOptions = parquetOptions
            });
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (!string.IsNullOrEmpty(_filePath))
        {
            var directory = Path.GetDirectoryName(_filePath);
            if (directory is not null && Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Benchmark(Baseline = true)]
    public async Task<int> Deserialize_All_And_Filter_In_Memory()
    {
        var rows = await ParquetSerializer.DeserializeAsync<BenchmarkRow>(_filePath);
        return rows.Count(row => row.Country == "DE" && row.Age >= 50);
    }

    [Benchmark]
    public async Task<int> Query_RowGroup_Pushdown()
    {
        var rows = await ParquetQuery
            .FromFile<BenchmarkRow>(_filePath)
            .Where(row => row.Country == "DE" && row.Age >= 50)
            .ToListAsync();

        return rows.Count;
    }

    [Benchmark]
    public async Task<int> Query_Page_Prune_And_Project()
    {
        var rows = await ParquetQuery
            .FromFile<BenchmarkRow>(_filePath)
            .Pushdown(filter => filter.Eq(row => row.Country, "DE").Ge(row => row.Age, 50))
            .Select(row => row.Name)
            .ToListAsync();

        return rows.Count;
    }
}

public sealed class BenchmarkRow
{
    public int Id { get; set; }

    public string Country { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public int Age { get; set; }

    public int Score { get; set; }
}
