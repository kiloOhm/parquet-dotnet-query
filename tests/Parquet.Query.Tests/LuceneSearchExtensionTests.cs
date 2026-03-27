using Parquet.Query.Extensions.Search;
using Parquet.Query.Extensions.Writing;
using Parquet.Query.Extensions.Writing.Attributes;
using Parquet.Serialization;

namespace Parquet.Query.Tests;

public sealed class LuceneSearchExtensionTests : IAsyncLifetime
{
    private readonly string _tempDirectory = Path.Combine(Path.GetTempPath(), "Parquet.Query.Lucene.Tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task WriteAsync_builds_footer_term_index_and_fuzzy_query_prunes_row_groups()
    {
        var filePath = Path.Combine(_tempDirectory, "lucene-fuzzy.parquet");

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new SearchRow { Id = 1, Name = "Berlin Travel" },
                new SearchRow { Id = 2, Name = "Hamburg Harbor" },
                new SearchRow { Id = 3, Name = "Tokyo Lights" },
                new SearchRow { Id = 4, Name = "Osaka Castle" }
            },
            filePath,
            indexingStrategies: new[] { new LuceneFooterIndexingStrategy() },
            serializerOptions: new ParquetSerializerOptions
            {
                RowGroupSize = 2
            });

        var query = ParquetQuery
            .FromFile<SearchRow>(filePath)
            .LuceneFuzzy(row => row.Name, "berln", maxEdits: 1, prefixLength: 1);

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { 1 }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(2, plan.RowGroups.Count);
        Assert.True(plan.RowGroups[0].ShouldRead);
        Assert.False(plan.RowGroups[1].ShouldRead);
        Assert.Contains(plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions), decision => decision.Source == "lucene");
    }

    [Fact]
    public async Task LuceneMatch_uses_analyzed_tokens_instead_of_full_field_values()
    {
        var filePath = Path.Combine(_tempDirectory, "lucene-match.parquet");

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new SearchRow { Id = 1, Name = "Berlin City Guide" },
                new SearchRow { Id = 2, Name = "Hamburg Harbor Notes" },
                new SearchRow { Id = 3, Name = "Tokyo Lights" },
                new SearchRow { Id = 4, Name = "Osaka Castle" }
            },
            filePath,
            indexingStrategies: new[] { new LuceneFooterIndexingStrategy() },
            serializerOptions: new ParquetSerializerOptions
            {
                RowGroupSize = 2
            });

        var rows = await ParquetQuery
            .FromFile<SearchRow>(filePath)
            .LuceneMatch(row => row.Name, "guide")
            .ToListAsync();

        Assert.Equal(new[] { 1 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task LuceneFuzzy_can_disable_transposition_matches()
    {
        var filePath = Path.Combine(_tempDirectory, "lucene-transpositions.parquet");

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new SearchRow { Id = 1, Name = "abcd" },
                new SearchRow { Id = 2, Name = "wxyz" }
            },
            filePath,
            indexingStrategies: new[] { new LuceneFooterIndexingStrategy() },
            serializerOptions: new ParquetSerializerOptions
            {
                RowGroupSize = 1
            });

        var withTranspositions = await ParquetQuery
            .FromFile<SearchRow>(filePath)
            .LuceneFuzzy(row => row.Name, "abdc", maxEdits: 1, transpositions: true)
            .ToListAsync();

        var withoutTranspositions = await ParquetQuery
            .FromFile<SearchRow>(filePath)
            .LuceneFuzzy(row => row.Name, "abdc", maxEdits: 1, transpositions: false)
            .ToListAsync();

        Assert.Equal(new[] { 1 }, withTranspositions.Select(row => row.Id).ToArray());
        Assert.Empty(withoutTranspositions);
    }

    [Fact]
    public async Task LuceneMatch_without_footer_index_falls_back_to_residual_row_filtering()
    {
        var filePath = Path.Combine(_tempDirectory, "lucene-no-index.parquet");

        await ParquetSerializer.SerializeAsync(
            new[]
            {
                new PlainSearchRow { Id = 1, Name = "Berlin Guide" },
                new PlainSearchRow { Id = 2, Name = "Tokyo Notes" },
                new PlainSearchRow { Id = 3, Name = "Harbor Maps" }
            },
            filePath,
            new ParquetSerializerOptions
            {
                RowGroupSize = 1
            });

        var query = ParquetQuery
            .FromFile<PlainSearchRow>(filePath)
            .WithLuceneSearch()
            .Pushdown(filter => filter.LuceneMatch(row => row.Name, "guide"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { 1 }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(3, plan.RowGroups.Count(rowGroup => rowGroup.ShouldRead));
        Assert.DoesNotContain(plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions), decision => decision.Source == "lucene");
    }

    [Fact]
    public async Task LuceneMatch_with_corrupt_footer_index_falls_back_to_residual_row_filtering()
    {
        var filePath = Path.Combine(_tempDirectory, "lucene-corrupt-index.parquet");

        await ParquetFileWriter.WriteAsync(
            new[]
            {
                new SearchRow { Id = 1, Name = "Berlin Guide" },
                new SearchRow { Id = 2, Name = "Tokyo Notes" }
            },
            filePath,
            indexingStrategies: new[] { new LuceneFooterIndexingStrategy() },
            serializerOptions: new ParquetSerializerOptions
            {
                RowGroupSize = 1
            });

        await SetFileMetadataAsync(
            filePath,
            new Dictionary<string, string>
            {
                ["parquet.query.lucene.v1/" + Uri.EscapeDataString("Name")] = "not-base64"
            });

        var query = ParquetQuery
            .FromFile<SearchRow>(filePath)
            .WithLuceneSearch()
            .Pushdown(filter => filter.LuceneMatch(row => row.Name, "guide"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { 1 }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(2, plan.RowGroups.Count(rowGroup => rowGroup.ShouldRead));
        Assert.DoesNotContain(plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions), decision => decision.Source == "lucene");
    }

    [Fact]
    public async Task LuceneMatch_rejects_multi_token_terms()
    {
        var filePath = Path.Combine(_tempDirectory, "lucene-invalid-term.parquet");
        await ParquetSerializer.SerializeAsync(Array.Empty<PlainSearchRow>(), filePath);

        var exception = Assert.Throws<ArgumentException>(() =>
            ParquetQuery
                .FromFile<PlainSearchRow>(filePath)
                .LuceneMatch(row => row.Name, "two words"));

        Assert.Contains("exactly one analyzed token", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LuceneFuzzy_rejects_invalid_max_edits_and_prefix_length_during_execution()
    {
        var filePath = Path.Combine(_tempDirectory, "lucene-invalid-fuzzy.parquet");

        await ParquetSerializer.SerializeAsync(
            new[]
            {
                new PlainSearchRow { Id = 1, Name = "berlin" }
            },
            filePath);

        var invalidMaxEditsQuery = ParquetQuery
            .FromFile<PlainSearchRow>(filePath)
            .WithLuceneSearch()
            .Pushdown(filter => filter.LuceneFuzzy(row => row.Name, "berlin", maxEdits: 3));

        var invalidPrefixQuery = ParquetQuery
            .FromFile<PlainSearchRow>(filePath)
            .WithLuceneSearch()
            .Pushdown(filter => filter.LuceneFuzzy(row => row.Name, "berlin", maxEdits: 1, prefixLength: -1));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => invalidMaxEditsQuery.ToListAsync());
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => invalidPrefixQuery.ToListAsync());
    }

    [Fact]
    public async Task LuceneFooterIndexingStrategy_rejects_non_string_columns()
    {
        var filePath = Path.Combine(_tempDirectory, "lucene-non-string.parquet");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ParquetFileWriter.WriteAsync(
                new[]
                {
                    new NumericSearchRow { Id = 1, Code = 42 }
                },
                filePath,
                indexingStrategies: new[] { new LuceneFooterIndexingStrategy() }));

        Assert.Contains("string columns only", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LuceneFooterIndexingStrategy_rejects_missing_columns()
    {
        var filePath = Path.Combine(_tempDirectory, "lucene-missing-column.parquet");
        var rows = new[] { new SearchRow { Id = 1, Name = "Berlin" } };

        var plan = await ParquetFileWriter.WriteAsync(rows, filePath);
        var context = new Parquet.Query.Extensions.Writing.Indexing.ParquetIndexingContext(filePath, plan);
        var descriptor = new ParquetIndexDescriptor(
            ParquetIndexKind.External,
            memberPath: nameof(SearchRow.Name),
            columnPath: "Missing",
            strategyName: LuceneIndexNames.StrategyName,
            order: 0,
            direction: null);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new LuceneFooterIndexingStrategy().ApplyAsync(context, descriptor).AsTask());

        Assert.Contains("was not found", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void LuceneFooterIndexingStrategy_can_handle_only_matching_external_descriptors()
    {
        var strategy = new LuceneFooterIndexingStrategy();

        Assert.Equal(LuceneIndexNames.StrategyName, strategy.Name);
        Assert.True(strategy.CanHandle(new ParquetIndexDescriptor(
            ParquetIndexKind.External,
            memberPath: "Name",
            columnPath: "Name",
            strategyName: LuceneIndexNames.StrategyName,
            order: 0,
            direction: null)));
        Assert.False(strategy.CanHandle(new ParquetIndexDescriptor(
            ParquetIndexKind.BloomFilter,
            memberPath: "Name",
            columnPath: "Name",
            strategyName: LuceneIndexNames.StrategyName,
            order: 0,
            direction: null)));
        Assert.False(strategy.CanHandle(new ParquetIndexDescriptor(
            ParquetIndexKind.External,
            memberPath: "Name",
            columnPath: "Name",
            strategyName: "other",
            order: 0,
            direction: null)));
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
    {
        var fileBytes = await System.IO.File.ReadAllBytesAsync(filePath);

        using var input = new MemoryStream(fileBytes, writable: false);
        using var reader = await Parquet.ParquetReader.CreateAsync(input);

        var footerMetadata = reader.Metadata!;
        footerMetadata.KeyValueMetadata = reader.CustomMetadata
            .Concat(metadata)
            .GroupBy(entry => entry.Key, StringComparer.Ordinal)
            .Select(group => new Parquet.Meta.KeyValue { Key = group.Key, Value = group.Last().Value })
            .ToList();

        var originalFooterLength = BitConverter.ToInt32(fileBytes, fileBytes.Length - 8);
        var footerStart = fileBytes.Length - 8 - originalFooterLength;

        using var output = new MemoryStream();
        await output.WriteAsync(fileBytes.AsMemory(0, footerStart));

        var footerType = typeof(Parquet.ParquetReader).Assembly.GetType("Parquet.File.ThriftFooter", throwOnError: true)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter could not be loaded.");
        var footer = Activator.CreateInstance(footerType, footerMetadata)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter could not be created.");
        var writeMethod = footerType.GetMethod(
            "Write",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic,
            binder: null,
            types: new[] { typeof(Stream) },
            modifiers: null)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter.Write(Stream) could not be found.");
        var newFooterLength = checked(Convert.ToInt32(writeMethod.Invoke(footer, new object[] { output })));
        await output.WriteAsync(BitConverter.GetBytes(newFooterLength));
        await output.WriteAsync(System.Text.Encoding.ASCII.GetBytes("PAR1"));

        await System.IO.File.WriteAllBytesAsync(filePath, output.ToArray());
    }

    private sealed class SearchRow
    {
        public int Id { get; set; }

        [ParquetExternalIndex(LuceneIndexNames.StrategyName)]
        public string Name { get; set; } = string.Empty;
    }

    private sealed class PlainSearchRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class NumericSearchRow
    {
        public int Id { get; set; }

        [ParquetExternalIndex(LuceneIndexNames.StrategyName)]
        public int Code { get; set; }
    }
}
