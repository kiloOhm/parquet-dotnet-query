using Parquet.Query.Extensions.Pooling;
using Parquet.Serialization;

namespace Parquet.Query.Tests;

public sealed class ParquetReaderPoolExtensionTests : IAsyncLifetime
{
    private readonly string _tempDirectory = Path.Combine(Path.GetTempPath(), "Parquet.Query.Pooling.Tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task RentAsync_reuses_reader_instances_for_the_same_file()
    {
        var filePath = Path.Combine(_tempDirectory, "reuse.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new PoolRow { Id = 1, Name = "alpha" },
                new PoolRow { Id = 2, Name = "bravo" }
            });

        await using var pool = new ParquetReaderPool();

        var firstLease = await pool.RentAsync(filePath);
        var firstReader = firstLease.Reader;
        await firstLease.DisposeAsync();

        await using var secondLease = await pool.RentAsync(filePath);

        Assert.Same(firstReader, secondLease.Reader);
    }

    [Fact]
    public async Task WithReaderPool_executes_queries_against_prewarmed_readers()
    {
        var filePath = Path.Combine(_tempDirectory, "query.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new PoolRow { Id = 1, Name = "alpha" },
                new PoolRow { Id = 2, Name = "bravo" },
                new PoolRow { Id = 3, Name = "charlie" },
                new PoolRow { Id = 4, Name = "delta" }
            });

        await using var pool = new ParquetReaderPool(new ParquetReaderPoolOptions
        {
            MaxReadersPerFile = 2
        });
        await pool.PrewarmAsync(filePath, readerCount: 2);

        var query = ParquetQuery
            .FromFile<PoolRow>(filePath)
            .WithReaderPool(pool)
            .Where(row => row.Id >= 3);

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { 3, 4 }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(2, plan.RowGroups.Count);
    }

    [Fact]
    public async Task BlockFileAsync_waits_for_active_readers_and_allows_file_replacement()
    {
        var filePath = Path.Combine(_tempDirectory, "replace.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new PoolRow { Id = 1, Name = "before-a" },
                new PoolRow { Id = 2, Name = "before-b" }
            });

        await using var pool = new ParquetReaderPool();

        var activeLease = await pool.RentAsync(filePath);
        try
        {
            var blockTask = pool.BlockFileAsync(filePath);
            await Task.Delay(100);
            Assert.False(blockTask.IsCompleted);

            await activeLease.DisposeAsync();

            await using var fileBlock = await blockTask;
            await WriteRowsAsync(
                filePath,
                new[]
                {
                    new PoolRow { Id = 10, Name = "after-a" },
                    new PoolRow { Id = 11, Name = "after-b" }
                });
        }
        finally
        {
            await activeLease.DisposeAsync();
        }

        var rows = await ParquetQuery
            .FromFile<PoolRow>(filePath)
            .WithReaderPool(pool)
            .ToListAsync();

        Assert.Equal(new[] { 10, 11 }, rows.Select(row => row.Id).ToArray());
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

    private static Task WriteRowsAsync(string filePath, IReadOnlyCollection<PoolRow> rows) =>
        ParquetSerializer.SerializeAsync(
            rows,
            filePath,
            new ParquetSerializerOptions
            {
                RowGroupSize = 2
            });

    private sealed class PoolRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
