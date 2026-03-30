using Parquet;
using Parquet.Serialization;

namespace Parquet.Query.Tests;

public sealed class ParquetQueryCacheTests : IAsyncLifetime
{
    private readonly string _tempDirectory = Path.Combine(Path.GetTempPath(), "Parquet.Query.Cache.Tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task Default_query_cache_reuses_plans_for_equivalent_queries()
    {
        var filePath = await WriteRowsAsync("default-cache.parquet");
        await using var readerFactory = new CountingParquetReaderFactory();

        var firstQuery = CreateQuery(filePath, readerFactory);
        var secondQuery = CreateQuery(filePath, readerFactory);

        var firstPlan = await firstQuery.PlanAsync();
        var secondPlan = await secondQuery.PlanAsync();

        Assert.Single(firstPlan.Files);
        Assert.Single(secondPlan.Files);
        Assert.Equal(1, readerFactory.RentCount);
    }

    [Fact]
    public async Task WithoutQueryCache_forces_replanning_for_equivalent_queries()
    {
        var filePath = await WriteRowsAsync("without-cache.parquet");
        await using var readerFactory = new CountingParquetReaderFactory();

        var firstQuery = CreateQuery(filePath, readerFactory).WithoutQueryCache();
        var secondQuery = CreateQuery(filePath, readerFactory).WithoutQueryCache();

        await firstQuery.PlanAsync();
        await secondQuery.PlanAsync();

        Assert.Equal(2, readerFactory.RentCount);
    }

    [Fact]
    public async Task WithQueryCache_uses_attached_external_cache_instance()
    {
        var filePath = await WriteRowsAsync("external-cache.parquet");
        await using var readerFactory = new CountingParquetReaderFactory();
        var cache = new RecordingQueryCache();

        var firstQuery = CreateQuery(filePath, readerFactory).WithQueryCache(cache);
        var secondQuery = CreateQuery(filePath, readerFactory).WithQueryCache(cache);

        await firstQuery.PlanAsync();
        await secondQuery.PlanAsync();

        Assert.Equal(1, readerFactory.RentCount);
        Assert.Equal(2, cache.GetCount);
        Assert.Equal(1, cache.SetCount);
    }

    [Fact]
    public async Task LruParquetQueryCache_evicts_least_recently_used_entries()
    {
        var firstFilePath = await WriteRowsAsync("lru-first.parquet");
        var secondFilePath = await WriteRowsAsync("lru-second.parquet");
        await using var readerFactory = new CountingParquetReaderFactory();
        var cache = new LruParquetQueryCache(capacity: 1);

        await CreateQuery(firstFilePath, readerFactory)
            .WithQueryCache(cache)
            .PlanAsync();

        await CreateQuery(secondFilePath, readerFactory)
            .WithQueryCache(cache)
            .PlanAsync();

        await CreateQuery(firstFilePath, readerFactory)
            .WithQueryCache(cache)
            .PlanAsync();

        Assert.Equal(3, readerFactory.RentCount);
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

    private ParquetQuery<TestRow, TestRow> CreateQuery(string filePath, IParquetReaderFactory readerFactory) =>
        ParquetQuery
            .FromFile<TestRow>(filePath)
            .WithReaderFactory(readerFactory)
            .Where(row => row.Age >= 18);

    private async Task<string> WriteRowsAsync(string fileName)
    {
        var filePath = Path.Combine(_tempDirectory, fileName);
        await ParquetSerializer.SerializeAsync(
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "DE", Name = "bravo", Age = 20 },
                new TestRow { Id = 3, Country = "US", Name = "charlie", Age = 30 }
            },
            filePath,
            new ParquetSerializerOptions
            {
                RowGroupSize = 1
            });
        return filePath;
    }

    private sealed class RecordingQueryCache : IParquetQueryCache
    {
        private readonly Dictionary<string, object> _entries = new(StringComparer.Ordinal);

        public int GetCount { get; private set; }

        public int SetCount { get; private set; }

        public ValueTask<object?> GetAsync(string key, CancellationToken cancellationToken = default)
        {
            GetCount++;
            _entries.TryGetValue(key, out object? value);
            return new ValueTask<object?>(value);
        }

        public ValueTask SetAsync(string key, object value, CancellationToken cancellationToken = default)
        {
            SetCount++;
            _entries[key] = value;
            return default;
        }
    }

    private sealed class CountingParquetReaderFactory : IParquetReaderFactory, IAsyncDisposable
    {
        public int RentCount { get; private set; }

        public async ValueTask<IParquetReaderLease> RentAsync(
            string filePath,
            ParquetOptions? parquetOptions = null,
            CancellationToken cancellationToken = default)
        {
            RentCount++;

            var stream = System.IO.File.OpenRead(filePath);
            try
            {
                var reader = await ParquetReader.CreateAsync(
                    stream,
                    parquetOptions,
                    leaveStreamOpen: false,
                    cancellationToken).ConfigureAwait(false);
                return new CountingLease(reader, stream);
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        public ValueTask DisposeAsync() => default;

        private sealed class CountingLease : IParquetReaderLease
        {
            private readonly Stream _stream;
            private bool _disposed;

            public CountingLease(ParquetReader reader, Stream stream)
            {
                Reader = reader;
                _stream = stream;
            }

            public ParquetReader Reader { get; }

            public async ValueTask DisposeAsync()
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                Reader.Dispose();
                _stream.Dispose();
            }
        }
    }
}
