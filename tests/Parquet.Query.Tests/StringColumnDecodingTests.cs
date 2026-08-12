using Parquet.Serialization;
#if PARQUET_V6
using TestParquetSerializerOptions = Parquet.Query.Tests.CompatibilityParquetOptions;
#else
using TestParquetSerializerOptions = Parquet.Serialization.ParquetSerializerOptions;
#endif

namespace Parquet.Query.Tests;

/// <summary>
/// Covers string column decoding, which reuses one instance per distinct value instead of allocating
/// one per row. The reuse cache and the null expansion it has to do are easy places to return the
/// wrong value for a row, so the awkward cases are pinned here.
/// </summary>
public sealed class StringColumnDecodingTests : IAsyncLifetime
{
    private readonly string _tempDirectory = Path.Combine(
        Path.GetTempPath(),
        "Parquet.Query.Tests.StringDecoding",
        Guid.NewGuid().ToString("N"));

    public sealed class StringRow
    {
        public int Id { get; set; }

        public string? Value { get; set; }

        public string Required { get; set; } = string.Empty;
    }

    [Fact]
    public async Task Decodes_repeated_nullable_and_empty_values_in_row_order()
    {
        var expected = new[]
        {
            "repeat", "repeat", "repeat",
            null,
            string.Empty,
            "repeat",
            "other",
            null,
            "other",
            "unicode-äöü-中文",
            "repeat",
            "tail",
        };

        var rows = expected
            .Select((value, index) => new StringRow
            {
                Id = index,
                Value = value,
                Required = value ?? "was-null",
            })
            .ToArray();

        // A row group size below the row count forces several row groups, so the per-row-group reuse
        // cache is exercised more than once and any cross-row-group leakage would show up.
        var filePath = await WriteAsync("mixed.parquet", rows, rowGroupSize: 5);

        var read = await ParquetQuery
            .FromFile<StringRow>(filePath)
            .ToListAsync();

        Assert.Equal(rows.Length, read.Count);
        var ordered = read.OrderBy(row => row.Id).ToArray();
        for (var index = 0; index < expected.Length; index++)
        {
            Assert.Equal(expected[index], ordered[index].Value);
            Assert.Equal(expected[index] ?? "was-null", ordered[index].Required);
        }
    }

#if PARQUET_V6
    // Instance reuse is a property of the v6 decoding path; on the legacy path the reader hands back
    // whatever it allocated per row, so the assertion would not describe that build.
    [Fact]
    public async Task Reuses_one_instance_per_distinct_value_within_a_row_group()
    {
        var rows = Enumerable.Range(0, 200)
            .Select(index => new StringRow
            {
                Id = index,
                Value = index % 2 == 0 ? "even" : "odd",
                Required = "constant",
            })
            .ToArray();

        var filePath = await WriteAsync("repeats.parquet", rows, rowGroupSize: 200);

        var read = await ParquetQuery
            .FromFile<StringRow>(filePath)
            .ToListAsync();

        Assert.Equal(200, read.Count);

        // Reference equality is the point of the decoder: without reuse this would be 200 instances.
        var distinctRequired = CountDistinctInstances(read.Select(row => row.Required));
        Assert.Equal(1, distinctRequired);

        var distinctValues = CountDistinctInstances(read.Select(row => row.Value!));
        Assert.Equal(2, distinctValues);
    }

    private static int CountDistinctInstances(IEnumerable<string> values)
    {
        var seen = new List<string>();
        foreach (var value in values)
        {
            if (!seen.Any(candidate => ReferenceEquals(candidate, value)))
            {
                seen.Add(value);
            }
        }

        return seen.Count;
    }
#endif

    [Fact]
    public async Task Filters_on_a_string_column_that_repeats()
    {
        var rows = Enumerable.Range(0, 300)
            .Select(index => new StringRow
            {
                Id = index,
                Value = $"group-{index % 5}",
                Required = "x",
            })
            .ToArray();

        var filePath = await WriteAsync("filter.parquet", rows, rowGroupSize: 64);

        var read = await ParquetQuery
            .FromFile<StringRow>(filePath)
            .Where(row => row.Value == "group-3")
            .ToListAsync();

        Assert.Equal(60, read.Count);
        Assert.All(read, row => Assert.Equal("group-3", row.Value));
        Assert.All(read, row => Assert.Equal(3, row.Id % 5));
    }

    [Fact]
    public async Task Decodes_values_longer_than_the_hash_sample()
    {
        // The reuse cache hashes only a bounded number of characters, so values sharing a long prefix
        // and differing late must still be told apart.
        var shared = new string('p', 64);
        var rows = Enumerable.Range(0, 40)
            .Select(index => new StringRow
            {
                Id = index,
                Value = shared + (index % 4),
                Required = "y",
            })
            .ToArray();

        var filePath = await WriteAsync("long.parquet", rows, rowGroupSize: 40);

        var read = await ParquetQuery
            .FromFile<StringRow>(filePath)
            .ToListAsync();

        var ordered = read.OrderBy(row => row.Id).ToArray();
        for (var index = 0; index < rows.Length; index++)
        {
            Assert.Equal(shared + (index % 4), ordered[index].Value);
        }
    }

    private async Task<string> WriteAsync(string fileName, StringRow[] rows, int rowGroupSize)
    {
        var filePath = Path.Combine(_tempDirectory, fileName);
        var serializerOptions = new TestParquetSerializerOptions
        {
            RowGroupSize = rowGroupSize,
        };

        await ParquetSerializer.SerializeAsync(rows, filePath, serializerOptions);
        return filePath;
    }

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_tempDirectory);
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        if (Directory.Exists(_tempDirectory))
        {
            Directory.Delete(_tempDirectory, recursive: true);
        }

        return Task.CompletedTask;
    }
}
