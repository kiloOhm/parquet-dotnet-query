using Parquet;
using Parquet.Query.Extensions.Indexing;
using Parquet.Query.Internal;
using Parquet.Query.Extensions.Writing;
using Parquet.Query.Extensions.Writing.Attributes;
using Parquet.Query.Extensions.Writing.Indexing;
using Parquet.Serialization;

namespace Parquet.Query.Tests;

public sealed class FooterIndexExtensionTests : IAsyncLifetime
{
    private readonly string _tempDirectory = Path.Combine(Path.GetTempPath(), "Parquet.Query.FooterIndex.Tests", Guid.NewGuid().ToString("N"));

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
    public async Task WriteAsync_builds_footer_bitmap_index_for_footer_encrypted_files()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-bitmap-encrypted.parquet");
        const string footerKey = "0123456789ABCDEF";

        if (!await TryWriteEncryptedAsync(() => ParquetFileWriter.WriteAsync(
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
            })))
        {
            return;
        }

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
    public async Task WriteAsync_preserves_signed_plaintext_footers_for_footer_indexes()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-bitmap-signed.parquet");
        const string footerSigningKey = "0011223344556677";
        const string aadPrefix = "footer-index-tests";

        if (!await TryWriteEncryptedAsync(() => ParquetFileWriter.WriteAsync(
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
                    UsePlaintextFooter = true,
                    FooterSigningKey = footerSigningKey,
                    AADPrefix = aadPrefix,
                    SupplyAadPrefix = true
                }
            })))
        {
            return;
        }

        var query = ParquetQuery
            .FromFile<FooterBitmapRow>(filePath)
            .UsePlaintextFooter()
            .WithFooterSigningKey(footerSigningKey)
            .WithAadPrefix(aadPrefix, supplyOutOfBand: true)
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
    public async Task WriteAsync_builds_footer_sort_order_metadata()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-sort-order.parquet");

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new FooterSortOrderRow { Id = "1", CreatedAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc), Amount = 10m },
                new FooterSortOrderRow { Id = "2", CreatedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc), Amount = 20m }
            },
            filePath,
            indexingStrategies: new IParquetIndexingStrategy[] { new FooterSortOrderIndexingStrategy() });

        using var stream = System.IO.File.OpenRead(filePath);
        using var reader = await Parquet.ParquetReader.CreateAsync(stream);
        var sortOrderPayload = reader.CustomMetadata
            .FirstOrDefault(entry => string.Equals(entry.Key, "parquet.query.index.sortorder.v1", StringComparison.Ordinal))
            .Value;

        Assert.NotNull(sortOrderPayload);

        var model = ParquetFooterMetadata.TryDeserialize<FooterSortOrderTestModel>(sortOrderPayload);

        Assert.NotNull(model);
        Assert.Equal(2, model!.Columns.Count);

        Assert.Equal("CreatedAt", model.Columns[0].ColumnPath);
        Assert.Equal(1, model.Columns[0].Order);
        Assert.Equal("descending", model.Columns[0].Direction);

        Assert.Equal("Amount", model.Columns[1].ColumnPath);
        Assert.Equal(2, model.Columns[1].Order);
        Assert.Equal("ascending", model.Columns[1].Direction);
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

    [Fact]
    public async Task FooterBitmapIndexingStrategy_warns_when_column_cardinality_is_too_high()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-bitmap-warning.parquet");

        string warning = string.Empty;
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => CaptureConsoleErrorAsync(
            () => ParquetFileWriter.WriteAsync(
                new[]
                {
                    new FooterBitmapHighCardinalityRow { Code = "A" },
                    new FooterBitmapHighCardinalityRow { Code = "B" },
                    new FooterBitmapHighCardinalityRow { Code = "C" }
                },
                filePath,
                indexingStrategies: new[] { new FooterBitmapIndexingStrategy(maxDistinctValues: 2) }),
            captured => warning = captured));

        Assert.Contains("low-cardinality columns", exception.Message, StringComparison.Ordinal);
        Assert.Contains("footer-bitmap", warning, StringComparison.Ordinal);
        Assert.Contains("lucene", warning, StringComparison.Ordinal);
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

    private static async Task<bool> TryWriteEncryptedAsync(Func<Task> action)
    {
#if NET48
        var exception = await Assert.ThrowsAsync<PlatformNotSupportedException>(action);
        Assert.Contains("AES-GCM", exception.Message, StringComparison.Ordinal);
        return false;
#else
        await action();
        return true;
#endif
    }

    private static async Task SetFileMetadataAsync(string filePath, IReadOnlyDictionary<string, string> metadata)
        => await ParquetFooterMetadata.WriteAsync(filePath, metadata);

    private static string CaptureConsoleError(Func<Task> action)
    {
        var writer = new StringWriter();
        var previous = Console.Error;
        Console.SetError(writer);
        try
        {
            action().GetAwaiter().GetResult();
            return writer.ToString();
        }
        finally
        {
            Console.SetError(previous);
            writer.Dispose();
        }
    }

    private static async Task CaptureConsoleErrorAsync(Func<Task> action, Action<string> onCompleted)
    {
        var writer = new StringWriter();
        var previous = Console.Error;
        Console.SetError(writer);
        try
        {
            await action().ConfigureAwait(false);
        }
        finally
        {
            Console.SetError(previous);
            onCompleted(writer.ToString());
            writer.Dispose();
        }
    }

    private sealed class FooterBitmapRow
    {
        public string Id { get; set; } = string.Empty;

        [ParquetFooterBitmapIndex]
        public string Group { get; set; } = string.Empty;
    }

    private sealed class PlainFooterBitmapRow
    {
        public string Id { get; set; } = string.Empty;

        public string Group { get; set; } = string.Empty;
    }

    private sealed class FooterBitmapHighCardinalityRow
    {
        [ParquetFooterBitmapIndex]
        public string Code { get; set; } = string.Empty;
    }

    private sealed class FooterSortOrderRow
    {
        public string Id { get; set; } = string.Empty;

        [ParquetSortKey(priority: 1, Direction = ParquetSortDirection.Descending)]
        public DateTime CreatedAt { get; set; }

        [ParquetSortKey(priority: 2)]
        public decimal Amount { get; set; }
    }

    private sealed class FooterSortOrderTestModel
    {
        public List<FooterSortOrderTestColumnModel> Columns { get; set; } = [];
    }

    private sealed class FooterSortOrderTestColumnModel
    {
        public string ColumnPath { get; set; } = string.Empty;
        public int Order { get; set; }
        public string Direction { get; set; } = string.Empty;
    }
}
