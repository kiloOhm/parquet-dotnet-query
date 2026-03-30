using Parquet;
using Parquet.Query.Extensions.Indexing;
using Parquet.Query.Internal;
using Parquet.Query.Extensions.Writing;
using Parquet.Query.Extensions.Writing.Attributes;
using Parquet.Serialization;

namespace Parquet.Query.Tests;

public sealed class FooterIndexExtensionTests : IAsyncLifetime
{
    private readonly string _tempDirectory = Path.Combine(Path.GetTempPath(), "Parquet.Query.FooterIndex.Tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task WriteAsync_builds_footer_hash_index_and_equality_query_prunes_row_groups()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-hash.parquet");

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new FooterHashRow { Id = "key-100", Group = "north" },
                new FooterHashRow { Id = "key-300", Group = "west" },
                new FooterHashRow { Id = "key-200", Group = "south" },
                new FooterHashRow { Id = "key-400", Group = "east" }
            },
            filePath,
            indexingStrategies: new[] { new FooterHashIndexingStrategy(bucketCount: 65536) },
            serializerOptions: new ParquetSerializerOptions
            {
                RowGroupSize = 2
            });

        var query = ParquetQuery
            .FromFile<FooterHashRow>(filePath)
            .WithFooterIndexes()
            .Pushdown(filter => filter.Eq(row => row.Id, "key-200"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { "key-200" }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(2, plan.RowGroups.Count);
        Assert.False(plan.RowGroups[0].ShouldRead);
        Assert.True(plan.RowGroups[1].ShouldRead);
        Assert.Contains(plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions), decision => decision.Source.Contains("footer-hash", StringComparison.Ordinal));
    }

    [Fact]
    public async Task WriteAsync_builds_footer_bitmap_index_and_equality_query_prunes_row_groups()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-bitmap.parquet");

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new FooterBitmapRow { Id = "1", Group = "alpha" },
                new FooterBitmapRow { Id = "2", Group = "charlie" },
                new FooterBitmapRow { Id = "3", Group = "bravo" },
                new FooterBitmapRow { Id = "4", Group = "delta" }
            },
            filePath,
            indexingStrategies: new[] { new FooterBitmapIndexingStrategy() },
            serializerOptions: new ParquetSerializerOptions
            {
                RowGroupSize = 2
            });

        var query = ParquetQuery
            .FromFile<FooterBitmapRow>(filePath)
            .WithFooterIndexes()
            .Pushdown(filter => filter.Eq(row => row.Group, "bravo"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { "3" }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(2, plan.RowGroups.Count);
        Assert.False(plan.RowGroups[0].ShouldRead);
        Assert.True(plan.RowGroups[1].ShouldRead);
        Assert.Contains(plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions), decision => decision.Source.Contains("footer-bitmap", StringComparison.Ordinal));
    }

    [Fact]
    public async Task WriteAsync_builds_footer_hash_index_for_footer_encrypted_files()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-hash-encrypted.parquet");
        const string footerKey = "0123456789ABCDEF";

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new FooterHashRow { Id = "key-100", Group = "north" },
                new FooterHashRow { Id = "key-300", Group = "west" },
                new FooterHashRow { Id = "key-200", Group = "south" },
                new FooterHashRow { Id = "key-400", Group = "east" }
            },
            filePath,
            indexingStrategies: new[] { new FooterHashIndexingStrategy(bucketCount: 65536) },
            serializerOptions: new ParquetSerializerOptions
            {
                RowGroupSize = 2,
                ParquetOptions = new ParquetOptions
                {
                    FooterEncryptionKey = footerKey
                }
            });

        var query = ParquetQuery
            .FromFile<FooterHashRow>(filePath)
            .WithFooterKey(footerKey)
            .WithFooterIndexes()
            .Pushdown(filter => filter.Eq(row => row.Id, "key-200"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { "key-200" }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(2, plan.RowGroups.Count);
        Assert.False(plan.RowGroups[0].ShouldRead);
        Assert.True(plan.RowGroups[1].ShouldRead);
        Assert.Contains(plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions), decision => decision.Source.Contains("footer-hash", StringComparison.Ordinal));
    }

    [Fact]
    public async Task WriteAsync_builds_footer_bitmap_index_for_footer_encrypted_files()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-bitmap-encrypted.parquet");
        const string footerKey = "0123456789ABCDEF";

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new FooterBitmapRow { Id = "1", Group = "alpha" },
                new FooterBitmapRow { Id = "2", Group = "charlie" },
                new FooterBitmapRow { Id = "3", Group = "bravo" },
                new FooterBitmapRow { Id = "4", Group = "delta" }
            },
            filePath,
            indexingStrategies: new[] { new FooterBitmapIndexingStrategy() },
            serializerOptions: new ParquetSerializerOptions
            {
                RowGroupSize = 2,
                ParquetOptions = new ParquetOptions
                {
                    FooterEncryptionKey = footerKey
                }
            });

        var query = ParquetQuery
            .FromFile<FooterBitmapRow>(filePath)
            .WithFooterKey(footerKey)
            .WithFooterIndexes()
            .Pushdown(filter => filter.Eq(row => row.Group, "bravo"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { "3" }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(2, plan.RowGroups.Count);
        Assert.False(plan.RowGroups[0].ShouldRead);
        Assert.True(plan.RowGroups[1].ShouldRead);
        Assert.Contains(plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions), decision => decision.Source.Contains("footer-bitmap", StringComparison.Ordinal));
    }

    [Fact]
    public async Task WithFooterIndexes_without_footer_metadata_falls_back_to_standard_pruning()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-none.parquet");

        await ParquetSerializer.SerializeAsync(
            new[]
            {
                new PlainFooterBitmapRow { Id = "1", Group = "alpha" },
                new PlainFooterBitmapRow { Id = "2", Group = "charlie" },
                new PlainFooterBitmapRow { Id = "3", Group = "bravo" },
                new PlainFooterBitmapRow { Id = "4", Group = "delta" }
            },
            filePath,
            new ParquetSerializerOptions
            {
                RowGroupSize = 2
            });

        var query = ParquetQuery
            .FromFile<PlainFooterBitmapRow>(filePath)
            .WithFooterIndexes()
            .Pushdown(filter => filter.Eq(row => row.Group, "bravo"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { "3" }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(2, plan.RowGroups.Count(rowGroup => rowGroup.ShouldRead));
        Assert.DoesNotContain(plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions), decision => decision.Source.Contains("footer-", StringComparison.Ordinal));
    }

    [Fact]
    public async Task WithFooterIndexes_with_corrupt_bitmap_metadata_falls_back_to_standard_pruning()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-corrupt.parquet");

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new FooterBitmapRow { Id = "1", Group = "alpha" },
                new FooterBitmapRow { Id = "2", Group = "charlie" },
                new FooterBitmapRow { Id = "3", Group = "bravo" },
                new FooterBitmapRow { Id = "4", Group = "delta" }
            },
            filePath,
            indexingStrategies: new[] { new FooterBitmapIndexingStrategy() },
            serializerOptions: new ParquetSerializerOptions
            {
                RowGroupSize = 2
            });

        await SetFileMetadataAsync(
            filePath,
            new Dictionary<string, string>
            {
                ["parquet.query.index.bitmap.v1/" + Uri.EscapeDataString("Group")] = "not-base64"
            });

        var query = ParquetQuery
            .FromFile<FooterBitmapRow>(filePath)
            .WithFooterIndexes()
            .Pushdown(filter => filter.Eq(row => row.Group, "bravo"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { "3" }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(2, plan.RowGroups.Count(rowGroup => rowGroup.ShouldRead));
        Assert.DoesNotContain(plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions), decision => decision.Source.Contains("footer-", StringComparison.Ordinal));
    }

    [Fact]
    public async Task FooterHashIndexingStrategy_rejects_non_string_columns()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-hash-invalid.parquet");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ParquetFileWriter.WriteAsync(
                new[]
                {
                    new NumericHashRow { Id = 42 }
                },
                filePath,
                indexingStrategies: new[] { new FooterHashIndexingStrategy() }));

        Assert.Contains("string columns only", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FooterBitmapIndexingStrategy_rejects_high_cardinality_columns()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-bitmap-invalid.parquet");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ParquetFileWriter.WriteAsync(
                new[]
                {
                    new FooterBitmapHighCardinalityRow { Code = "A" },
                    new FooterBitmapHighCardinalityRow { Code = "B" },
                    new FooterBitmapHighCardinalityRow { Code = "C" }
                },
                filePath,
                indexingStrategies: new[] { new FooterBitmapIndexingStrategy(maxDistinctValues: 2) }));

        Assert.Contains("low-cardinality columns", exception.Message, StringComparison.Ordinal);
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

    private static async Task SetFileMetadataAsync(string filePath, IReadOnlyDictionary<string, string> metadata)
        => await ParquetFooterMetadata.WriteAsync(filePath, metadata);

    private sealed class FooterHashRow
    {
        [ParquetExternalIndex(FooterIndexNames.HashStrategyName)]
        public string Id { get; set; } = string.Empty;

        public string Group { get; set; } = string.Empty;
    }

    private sealed class NumericHashRow
    {
        [ParquetExternalIndex(FooterIndexNames.HashStrategyName)]
        public int Id { get; set; }
    }

    private sealed class FooterBitmapRow
    {
        public string Id { get; set; } = string.Empty;

        [ParquetExternalIndex(FooterIndexNames.BitmapStrategyName)]
        public string Group { get; set; } = string.Empty;
    }

    private sealed class PlainFooterBitmapRow
    {
        public string Id { get; set; } = string.Empty;

        public string Group { get; set; } = string.Empty;
    }

    private sealed class FooterBitmapHighCardinalityRow
    {
        [ParquetExternalIndex(FooterIndexNames.BitmapStrategyName)]
        public string Code { get; set; } = string.Empty;
    }
}
