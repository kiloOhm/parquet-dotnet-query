using Parquet.Query.Extensions.Writing;
using Parquet.Query.Extensions.Writing.Indexing;
using Parquet.Query.Internal;
using Parquet.Schema;

namespace Parquet.Query.Extensions.Search;

/// <summary>
/// Builds a Lucene-style term dictionary index and stores it in parquet footer metadata.
/// </summary>
public sealed class LuceneFooterIndexingStrategy : IParquetIndexingStrategy
{
    /// <inheritdoc />
    public string Name => LuceneIndexNames.StrategyName;

    /// <inheritdoc />
    public bool CanHandle(ParquetIndexDescriptor descriptor) =>
        descriptor.Kind == ParquetIndexKind.External &&
        string.Equals(descriptor.StrategyName, LuceneIndexNames.StrategyName, StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public async ValueTask ApplyAsync(
        ParquetIndexingContext context,
        ParquetIndexDescriptor descriptor,
        CancellationToken cancellationToken = default)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (descriptor is null)
        {
            throw new ArgumentNullException(nameof(descriptor));
        }

        var parquetOptions = ParquetOptionsFactory.Clone(context.WritePlan.SerializerOptions.ParquetOptions);
        var index = await BuildIndexAsync(context.FilePath, descriptor.ColumnPath, parquetOptions, cancellationToken).ConfigureAwait(false);
        var metadataKey = LuceneFooterIndexStorage.GetMetadataKey(descriptor.ColumnPath);
        var metadataValue = LuceneFooterIndexStorage.Serialize(index);
        await LuceneFooterIndexStorage.WriteToFooterAsync(context.FilePath, metadataKey, metadataValue, parquetOptions, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<LuceneFooterIndexModel> BuildIndexAsync(
        string filePath,
        string columnPath,
        Parquet.ParquetOptions? parquetOptions,
        CancellationToken cancellationToken)
    {
        using var stream = System.IO.File.OpenRead(filePath);
        using var reader = await Parquet.ParquetReader.CreateAsync(
            stream,
            parquetOptions,
            leaveStreamOpen: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var dataField = reader.Schema.GetDataFields()
            .FirstOrDefault(field => string.Equals(field.Path.ToString(), columnPath, StringComparison.Ordinal));
        if (dataField is null)
        {
            throw new InvalidOperationException($"Column '{columnPath}' was not found in '{filePath}'.");
        }

        var fieldType = Nullable.GetUnderlyingType(dataField.ClrType) ?? dataField.ClrType;
        if (fieldType != typeof(string))
        {
            throw new InvalidOperationException($"Lucene indexes currently support string columns only. Column '{columnPath}' is '{fieldType.Name}'.");
        }

        var rowGroupTerms = new List<HashSet<string>>(reader.RowGroupCount);
        var allTerms = new SortedSet<string>(StringComparer.Ordinal);

        for (var rowGroupIndex = 0; rowGroupIndex < reader.RowGroupCount; rowGroupIndex++)
        {
            using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
            var column = await rowGroupReader.ReadColumnAsync(dataField, cancellationToken).ConfigureAwait(false);
            var terms = ExtractTerms(column.Data);
            rowGroupTerms.Add(terms);
            allTerms.UnionWith(terms);
        }

        var orderedTerms = allTerms.ToArray();
        var ordinalsByTerm = orderedTerms
            .Select((term, ordinal) => (term, ordinal))
            .ToDictionary(pair => pair.term, pair => pair.ordinal, StringComparer.Ordinal);

        return new LuceneFooterIndexModel
        {
            ColumnPath = columnPath,
            Terms = orderedTerms.ToList(),
            RowGroups = rowGroupTerms
                .Select(terms => terms.Select(term => ordinalsByTerm[term]).OrderBy(ordinal => ordinal).ToArray())
                .ToList()
        };
    }

    private static HashSet<string> ExtractTerms(Array data)
    {
        var terms = new HashSet<string>(StringComparer.Ordinal);
        for (var index = 0; index < data.Length; index++)
        {
            if (data.GetValue(index) is not string value)
            {
                continue;
            }

            foreach (var token in LuceneTextAnalyzer.Analyze(value))
            {
                terms.Add(token);
            }
        }

        return terms;
    }
}
