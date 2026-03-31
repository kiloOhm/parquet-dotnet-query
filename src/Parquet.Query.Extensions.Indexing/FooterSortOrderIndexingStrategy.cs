using Parquet.Query.Extensions.Writing;
using Parquet.Query.Extensions.Writing.Indexing;
using Parquet.Query.Internal;

namespace Parquet.Query.Extensions.Indexing;

/// <summary>
/// Stores sort order metadata in parquet footer custom metadata.
/// </summary>
public sealed class FooterSortOrderIndexingStrategy : IParquetIndexingStrategy
{
    private string? _appliedFilePath;

    /// <inheritdoc />
    public string Name => FooterIndexNames.SortOrderStrategyName;

    /// <inheritdoc />
    public bool CanHandle(ParquetIndexDescriptor descriptor) =>
        descriptor.Kind == ParquetIndexKind.SortKey;

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

        // Sort order is file-level, not per-column. Write once for all SortKey descriptors.
        if (string.Equals(_appliedFilePath, context.FilePath, StringComparison.Ordinal))
        {
            return;
        }

        _appliedFilePath = context.FilePath;

        var sortKeys = context.WritePlan.IndexDescriptors
            .Where(d => d.Kind == ParquetIndexKind.SortKey)
            .OrderBy(d => d.Order)
            .Select(d => new FooterSortOrderColumnModel
            {
                ColumnPath = d.ColumnPath,
                Order = d.Order,
                Direction = d.Direction == ParquetSortDirection.Descending ? "descending" : "ascending"
            })
            .ToList();

        var model = new FooterSortOrderModel { Columns = sortKeys };
        var metadataKey = FooterIndexStorage.GetSortOrderMetadataKey();
        var metadataValue = FooterIndexStorage.Serialize(model);
        var parquetOptions = ParquetOptionsFactory.Clone(context.WritePlan.SerializerOptions.ParquetOptions);
        await FooterIndexStorage.WriteToFooterAsync(context.FilePath, metadataKey, metadataValue, parquetOptions, cancellationToken).ConfigureAwait(false);
    }
}
