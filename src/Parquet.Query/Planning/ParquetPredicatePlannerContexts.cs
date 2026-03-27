using Parquet;
using Parquet.Schema;

namespace Parquet.Query.Planning;

public sealed class ParquetRowGroupPlannerContext
{
    public ParquetRowGroupPlannerContext(
        string filePath,
        int rowGroupIndex,
        ParquetReader reader,
        IParquetRowGroupReader rowGroupReader,
        ParquetSchema schema,
        IReadOnlyDictionary<string, DataField> dataFields)
    {
        FilePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        RowGroupIndex = rowGroupIndex;
        Reader = reader ?? throw new ArgumentNullException(nameof(reader));
        RowGroupReader = rowGroupReader ?? throw new ArgumentNullException(nameof(rowGroupReader));
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        DataFields = dataFields ?? throw new ArgumentNullException(nameof(dataFields));
    }

    public string FilePath { get; }

    public int RowGroupIndex { get; }

    public ParquetReader Reader { get; }

    public IParquetRowGroupReader RowGroupReader { get; }

    public ParquetSchema Schema { get; }

    public IReadOnlyDictionary<string, DataField> DataFields { get; }
}

public sealed class ParquetPagePruningContext
{
    public ParquetPagePruningContext(
        string filePath,
        int rowGroupIndex,
        ParquetReader reader,
        IParquetRowGroupReader rowGroupReader,
        ParquetSchema schema,
        IReadOnlyDictionary<string, DataField> dataFields)
    {
        FilePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        RowGroupIndex = rowGroupIndex;
        Reader = reader ?? throw new ArgumentNullException(nameof(reader));
        RowGroupReader = rowGroupReader ?? throw new ArgumentNullException(nameof(rowGroupReader));
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        DataFields = dataFields ?? throw new ArgumentNullException(nameof(dataFields));
    }

    public string FilePath { get; }

    public int RowGroupIndex { get; }

    public ParquetReader Reader { get; }

    public IParquetRowGroupReader RowGroupReader { get; }

    public ParquetSchema Schema { get; }

    public IReadOnlyDictionary<string, DataField> DataFields { get; }
}
