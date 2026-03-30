using Parquet.Query.Extensions.Writing;
using Parquet.Query.Extensions.Writing.Indexing;
using Parquet.Query.Internal;

namespace Parquet.Query.Extensions.Indexing;

/// <summary>
/// Builds a low-cardinality bitmap index and stores it in parquet footer metadata.
/// </summary>
public sealed class FooterBitmapIndexingStrategy : IParquetIndexingStrategy
{
    /// <summary>
    /// Initializes a new footer bitmap indexing strategy.
    /// </summary>
    /// <param name="maxDistinctValues">The maximum number of distinct values allowed in the indexed column.</param>
    public FooterBitmapIndexingStrategy(int maxDistinctValues = 256)
    {
        if (maxDistinctValues <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDistinctValues), "The maximum number of distinct values must be positive.");
        }

        MaxDistinctValues = maxDistinctValues;
    }

    /// <inheritdoc />
    public string Name => FooterIndexNames.BitmapStrategyName;

    /// <summary>
    /// Gets the maximum number of distinct values allowed in the indexed column.
    /// </summary>
    public int MaxDistinctValues { get; }

    /// <inheritdoc />
    public bool CanHandle(ParquetIndexDescriptor descriptor) =>
        descriptor.Kind == ParquetIndexKind.External &&
        string.Equals(descriptor.StrategyName, FooterIndexNames.BitmapStrategyName, StringComparison.OrdinalIgnoreCase);

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
        var index = await BuildIndexAsync(context.FilePath, descriptor.ColumnPath, parquetOptions, MaxDistinctValues, cancellationToken).ConfigureAwait(false);
        var metadataKey = FooterIndexStorage.GetBitmapMetadataKey(descriptor.ColumnPath);
        var metadataValue = FooterIndexStorage.Serialize(index);
        await FooterIndexStorage.WriteToFooterAsync(context.FilePath, metadataKey, metadataValue, parquetOptions, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<FooterBitmapIndexModel> BuildIndexAsync(
        string filePath,
        string columnPath,
        Parquet.ParquetOptions? parquetOptions,
        int maxDistinctValues,
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
        if (!FooterIndexValueFormatter.IsSupportedType(fieldType))
        {
            FooterIndexDiagnostics.WarnBitmapUnsupportedType(columnPath, fieldType);
            throw new InvalidOperationException($"Footer bitmap indexes do not support '{fieldType.Name}' columns.");
        }

        var values = new Dictionary<string, HashSet<int>>(StringComparer.Ordinal);
        for (var rowGroupIndex = 0; rowGroupIndex < reader.RowGroupCount; rowGroupIndex++)
        {
            using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
            var column = await rowGroupReader.ReadColumnAsync(dataField, cancellationToken).ConfigureAwait(false);
            var rowGroupValues = new HashSet<string>(StringComparer.Ordinal);

            for (var index = 0; index < column.Data.Length; index++)
            {
                if (!FooterIndexValueFormatter.TryFormat(column.Data.GetValue(index), out var value))
                {
                    continue;
                }

                rowGroupValues.Add(value);
            }

            foreach (var value in rowGroupValues)
            {
                if (!values.TryGetValue(value, out var rowGroups))
                {
                    if (values.Count >= maxDistinctValues)
                    {
                        FooterIndexDiagnostics.WarnBitmapHighCardinality(columnPath, fieldType, values.Count + 1, maxDistinctValues);
                        throw new InvalidOperationException(
                            $"Footer bitmap indexes are intended for low-cardinality columns. Column '{columnPath}' exceeded the configured distinct value limit of {maxDistinctValues}.");
                    }

                    rowGroups = new HashSet<int>();
                    values[value] = rowGroups;
                }

                rowGroups.Add(rowGroupIndex);
            }
        }

        return new FooterBitmapIndexModel
        {
            ColumnPath = columnPath,
            Values = values
                .OrderBy(entry => entry.Key, StringComparer.Ordinal)
                .Select(entry => new FooterBitmapEntryModel
                {
                    Value = entry.Key,
                    RowGroups = entry.Value.OrderBy(rowGroup => rowGroup).ToArray()
                })
                .ToList()
        };
    }
}
