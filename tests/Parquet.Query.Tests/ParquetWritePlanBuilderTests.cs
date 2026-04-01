using System.IO.Compression;
using System.Text.Json.Serialization;
using Parquet;
using Parquet.Query.Extensions.Writing;
using Parquet.Query.Extensions.Writing.Attributes;
using Parquet.Query.Extensions.Writing.Indexing;
using Parquet.Serialization;
using Parquet.Serialization.Attributes;

namespace Parquet.Query.Tests;

public sealed class ParquetWritePlanBuilderTests : IAsyncLifetime
{
    private readonly string _tempDirectory = Path.Combine(Path.GetTempPath(), "Parquet.Query.Writing.Tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public void BuildSchema_uses_class_definition_and_serializer_attributes()
    {
        var schema = ParquetWritePlanBuilder.BuildSchema<OptimizedWriteRow>();

        var dataFields = schema.GetDataFields().Select(field => field.Path.ToString()).ToArray();

        Assert.Contains("id", dataFields);
        Assert.Contains("event_name", dataFields);
        Assert.Contains("Amount", dataFields);
        Assert.DoesNotContain("Ignored", dataFields);
    }

    [Fact]
    public void Build_collects_writer_options_and_index_descriptors()
    {
        var plan = ParquetWritePlanBuilder.Build<OptimizedWriteRow>();

        Assert.Equal(CompressionMethod.Zstd, plan.SerializerOptions.CompressionMethod);
#if NET48
        Assert.Equal(CompressionLevel.Optimal, plan.SerializerOptions.CompressionLevel);
#else
        Assert.Equal(CompressionLevel.SmallestSize, plan.SerializerOptions.CompressionLevel);
#endif
        Assert.Equal(256, plan.SerializerOptions.RowGroupSize);

        Assert.Contains(plan.Columns, column => column.MemberPath == nameof(OptimizedWriteRow.EventName) && column.ColumnPath == "event_name");
        Assert.Contains(plan.IndexDescriptors, descriptor => descriptor.Kind == ParquetIndexKind.BloomFilter && descriptor.ColumnPath == "event_name");
        Assert.Contains(plan.IndexDescriptors, descriptor => descriptor.Kind == ParquetIndexKind.SortKey && descriptor.ColumnPath == "CreatedAt" && descriptor.Order == 1);
        Assert.Contains(plan.IndexDescriptors, descriptor => descriptor.Kind == ParquetIndexKind.External && descriptor.MemberPath == "Customer.CustomerId" && descriptor.StrategyName == "btree");
    }

    [Fact]
    public void Build_allows_explicit_options_to_override_attribute_defaults()
    {
        var explicitOptions = new ParquetSerializerOptions
        {
            CompressionMethod = CompressionMethod.Gzip,
            CompressionLevel = CompressionLevel.Fastest,
            RowGroupSize = 32
        };

        var plan = ParquetWritePlanBuilder.Build<OptimizedWriteRow>(explicitOptions);

        Assert.Equal(CompressionMethod.Gzip, plan.SerializerOptions.CompressionMethod);
        Assert.Equal(CompressionLevel.Fastest, plan.SerializerOptions.CompressionLevel);
        Assert.Equal(32, plan.SerializerOptions.RowGroupSize);
    }

    [Fact]
    public void Build_reuses_cached_default_plan_and_keeps_serializer_options_immutable()
    {
        var first = ParquetWritePlanBuilder.Build<OptimizedWriteRow>();
        var second = ParquetWritePlanBuilder.Build<OptimizedWriteRow>();

        Assert.Same(first, second);

        var options = first.SerializerOptions;
        options.CompressionMethod = CompressionMethod.None;

        Assert.Equal(CompressionMethod.Zstd, first.SerializerOptions.CompressionMethod);
        Assert.Equal(CompressionMethod.Zstd, second.SerializerOptions.CompressionMethod);
    }

    [Fact]
    public async Task WriteAsync_accepts_prebuilt_plan_for_reuse()
    {
        Directory.CreateDirectory(_tempDirectory);
        var filePath = Path.Combine(_tempDirectory, "prebuilt.parquet");
        var plan = ParquetWritePlanBuilder.Build<OptimizedWriteRow>();

        var returnedPlan = await ParquetFileWriter.WriteAsync(
            new[]
            {
                new OptimizedWriteRow
                {
                    Id = 2,
                    EventName = "purchase",
                    CreatedAt = new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Utc),
                    Amount = 54.32m,
                    Customer = new OptimizedCustomer { CustomerId = "C-2" }
                }
            },
            filePath,
            plan);

        var rows = await ParquetSerializer.DeserializeAsync<OptimizedWriteRow>(filePath);

        Assert.Same(plan, returnedPlan);
        Assert.Single(rows);
        Assert.Equal("purchase", rows[0].EventName);
    }

    [Fact]
    public async Task WriteAsync_emits_bloom_filter_metadata_for_annotated_columns()
    {
        Directory.CreateDirectory(_tempDirectory);
        var filePath = Path.Combine(_tempDirectory, "bloom.parquet");

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new OptimizedWriteRow
                {
                    Id = 3,
                    EventName = "refund",
                    CreatedAt = new DateTime(2024, 3, 4, 5, 6, 7, DateTimeKind.Utc),
                    Amount = 99.01m,
                    Customer = new OptimizedCustomer { CustomerId = "C-3" }
                }
            },
            filePath);

        using var reader = await ParquetReader.CreateAsync(filePath);
        using var rowGroupReader = reader.OpenRowGroupReader(0);
        var eventNameField = reader.Schema.GetDataFields().Single(field => field.Path.ToString() == "event_name");
        var metadata = rowGroupReader.GetMetadata(eventNameField);

        Assert.NotNull(metadata?.MetaData?.BloomFilterOffset);
    }

    [Fact]
    public async Task WriteAsync_serializes_rows_and_dispatches_matching_index_strategies()
    {
        Directory.CreateDirectory(_tempDirectory);
        var filePath = Path.Combine(_tempDirectory, "optimized.parquet");
        var strategy = new RecordingIndexStrategy();

        var plan = await ParquetFileWriter.WriteAsync(
            new[]
            {
                new OptimizedWriteRow
                {
                    Id = 1,
                    EventName = "signup",
                    CreatedAt = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc),
                    Amount = 12.34m,
                    Customer = new OptimizedCustomer { CustomerId = "C-1" }
                }
            },
            filePath,
            indexingStrategies: new[] { strategy });

        var rows = await ParquetSerializer.DeserializeAsync<OptimizedWriteRow>(filePath);

        Assert.Single(rows);
        Assert.Equal("signup", rows[0].EventName);
        Assert.Equal(4, strategy.Applied.Count);
        Assert.All(strategy.Applied, applied => Assert.Equal(filePath, applied.FilePath));
        Assert.Contains(strategy.Applied, applied => applied.Descriptor.Kind == ParquetIndexKind.External && applied.Descriptor.MemberPath == "Customer.CustomerId");
        Assert.Equal(4, plan.IndexDescriptors.Count);
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

#if NET48
    [ParquetWriteOptions(CompressionMethod = CompressionMethod.Zstd, CompressionLevel = CompressionLevel.Optimal, RowGroupSize = 256)]
#else
    [ParquetWriteOptions(CompressionMethod = CompressionMethod.Zstd, CompressionLevel = CompressionLevel.SmallestSize, RowGroupSize = 256)]
#endif
    private sealed class OptimizedWriteRow
    {
        [JsonPropertyName("id")]
        public int Id { get; set; }

        [JsonPropertyName("event_name")]
        [ParquetRequired]
        [ParquetBloomFilter]
        [ParquetExternalIndex("hash", Order = 2)]
        public string EventName { get; set; } = string.Empty;

        [ParquetTimestamp(ParquetTimestampResolution.Microseconds)]
        [ParquetSortKey(priority: 1, Direction = ParquetSortDirection.Descending)]
        public DateTime CreatedAt { get; set; }

        [ParquetDecimal(20, 4)]
        public decimal Amount { get; set; }

        public OptimizedCustomer Customer { get; set; } = new();

        [JsonIgnore]
        public string Ignored { get; set; } = string.Empty;
    }

    private sealed class OptimizedCustomer
    {
        [ParquetExternalIndex("btree", Order = 1)]
        public string CustomerId { get; set; } = string.Empty;
    }

    private sealed class RecordingIndexStrategy : IParquetIndexingStrategy
    {
        public string Name => "recording";

        public List<(string FilePath, ParquetIndexDescriptor Descriptor)> Applied { get; } = [];

        public bool CanHandle(ParquetIndexDescriptor descriptor) => true;

        public ValueTask ApplyAsync(ParquetIndexingContext context, ParquetIndexDescriptor descriptor, CancellationToken cancellationToken = default)
        {
            Applied.Add((context.FilePath, descriptor));
            return default;
        }
    }
}
