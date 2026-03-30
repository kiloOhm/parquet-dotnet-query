using Parquet;
using Parquet.Schema;

namespace Parquet.Query.Planning;

/// <summary>
/// Supplies the metadata needed to evaluate a predicate against a row group.
/// </summary>
public sealed class ParquetRowGroupPlannerContext
{
    /// <summary>
    /// Initializes a new row-group planning context.
    /// </summary>
    /// <param name="filePath">The parquet file being planned.</param>
    /// <param name="rowGroupIndex">The zero-based row-group index.</param>
    /// <param name="reader">The open parquet reader for the file.</param>
    /// <param name="rowGroupReader">The open reader for the row group.</param>
    /// <param name="schema">The parquet schema.</param>
    /// <param name="dataFields">The schema data fields indexed by parquet column path.</param>
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

    /// <summary>
    /// Gets the parquet file being planned.
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// Gets the zero-based row-group index.
    /// </summary>
    public int RowGroupIndex { get; }

    /// <summary>
    /// Gets the open parquet reader for the file.
    /// </summary>
    public ParquetReader Reader { get; }

    /// <summary>
    /// Gets the open row-group reader.
    /// </summary>
    public IParquetRowGroupReader RowGroupReader { get; }

    /// <summary>
    /// Gets the parquet schema.
    /// </summary>
    public ParquetSchema Schema { get; }

    /// <summary>
    /// Gets the schema data fields indexed by parquet column path.
    /// </summary>
    public IReadOnlyDictionary<string, DataField> DataFields { get; }
}

/// <summary>
/// Supplies the metadata needed to prune pages within a row group.
/// </summary>
public sealed class ParquetPagePruningContext
{
    /// <summary>
    /// Initializes a new page-pruning context.
    /// </summary>
    /// <param name="filePath">The parquet file being planned.</param>
    /// <param name="rowGroupIndex">The zero-based row-group index.</param>
    /// <param name="reader">The open parquet reader for the file.</param>
    /// <param name="rowGroupReader">The open reader for the row group.</param>
    /// <param name="schema">The parquet schema.</param>
    /// <param name="dataFields">The schema data fields indexed by parquet column path.</param>
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

    /// <summary>
    /// Gets the parquet file being planned.
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// Gets the zero-based row-group index.
    /// </summary>
    public int RowGroupIndex { get; }

    /// <summary>
    /// Gets the open parquet reader for the file.
    /// </summary>
    public ParquetReader Reader { get; }

    /// <summary>
    /// Gets the open row-group reader.
    /// </summary>
    public IParquetRowGroupReader RowGroupReader { get; }

    /// <summary>
    /// Gets the parquet schema.
    /// </summary>
    public ParquetSchema Schema { get; }

    /// <summary>
    /// Gets the schema data fields indexed by parquet column path.
    /// </summary>
    public IReadOnlyDictionary<string, DataField> DataFields { get; }
}
