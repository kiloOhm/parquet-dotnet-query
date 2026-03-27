using Parquet.Schema;
using Parquet.Serialization;

namespace Parquet.Query.Extensions.Writing;

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

    public Type RowType { get; }

    public ParquetSchema Schema { get; }

    public ParquetSerializerOptions SerializerOptions => _serializerOptions.ToSerializerOptions();

    public IReadOnlyList<ParquetColumnPlan> Columns { get; }

    public IReadOnlyList<ParquetIndexDescriptor> IndexDescriptors { get; }

    internal ParquetSerializerOptions CreateSerializerOptions() => _serializerOptions.ToSerializerOptions();
}
