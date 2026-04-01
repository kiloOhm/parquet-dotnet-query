using Parquet.Schema;
using Parquet.Serialization;

namespace Parquet.Query.Extensions.Writing;

/// <summary>
/// Describes how a row type will be serialized to parquet, including schema, columns, and indexes.
/// </summary>
public sealed class ParquetWritePlan
{
    private readonly SerializerOptionsSnapshot _serializerOptions;

    internal ParquetWritePlan(
        Type rowType,
        ParquetSchema schema,
        SerializerOptionsSnapshot serializerOptions,
        IReadOnlyList<ParquetColumnPlan> columns,
        IReadOnlyList<ParquetIndexDescriptor> indexDescriptors)
    {
        RowType = rowType ?? throw new ArgumentNullException(nameof(rowType));
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _serializerOptions = serializerOptions ?? throw new ArgumentNullException(nameof(serializerOptions));
        Columns = columns ?? throw new ArgumentNullException(nameof(columns));
        IndexDescriptors = indexDescriptors ?? throw new ArgumentNullException(nameof(indexDescriptors));
    }

    /// <summary>
    /// Gets the CLR row type represented by the plan.
    /// </summary>
    public Type RowType { get; }

    /// <summary>
    /// Gets the parquet schema used when writing the row type.
    /// </summary>
    public ParquetSchema Schema { get; }

    /// <summary>
    /// Gets the serializer options that will be applied when writing.
    /// </summary>
    public ParquetSerializerOptions SerializerOptions => _serializerOptions.ToSerializerOptions();

    /// <summary>
    /// Gets the column mappings discovered for the row type.
    /// </summary>
    public IReadOnlyList<ParquetColumnPlan> Columns { get; }

    /// <summary>
    /// Gets the index descriptors discovered from write attributes.
    /// </summary>
    public IReadOnlyList<ParquetIndexDescriptor> IndexDescriptors { get; }

    internal ParquetSerializerOptions CreateSerializerOptions()
    {
        var serializerOptions = _serializerOptions.ToSerializerOptions();
        var bloomFilterColumns = IndexDescriptors
            .Where(descriptor => descriptor.Kind == ParquetIndexKind.BloomFilter)
            .Select(descriptor => GetBloomFilterColumnKey(descriptor.ColumnPath))
            .Distinct(StringComparer.Ordinal);

        using var enumerator = bloomFilterColumns.GetEnumerator();
        if (!enumerator.MoveNext())
        {
            return serializerOptions;
        }

        serializerOptions.ParquetOptions ??= new Parquet.ParquetOptions();
        do
        {
            if (!serializerOptions.ParquetOptions.BloomFilterOptionsByColumn.ContainsKey(enumerator.Current))
            {
                serializerOptions.ParquetOptions.BloomFilterOptionsByColumn[enumerator.Current] = new Parquet.ParquetOptions.BloomFilterOptions
                {
                    EnableBloomFilters = true
                };
            }
        }
        while (enumerator.MoveNext());

        return serializerOptions;
    }

    private static string GetBloomFilterColumnKey(string columnPath)
    {
        var separatorIndex = columnPath.LastIndexOf('.');
        return separatorIndex >= 0
            ? columnPath.Substring(separatorIndex + 1)
            : columnPath;
    }
}
