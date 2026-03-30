namespace Parquet.Query.Extensions.Writing;

/// <summary>
/// Identifies the kind of parquet index requested by a write plan.
/// </summary>
public enum ParquetIndexKind
{
    /// <summary>
    /// Uses parquet's built-in bloom filter support.
    /// </summary>
    BloomFilter,
    /// <summary>
    /// Marks a column as part of the parquet sort order.
    /// </summary>
    SortKey,
    /// <summary>
    /// Uses an external indexing strategy to persist custom metadata.
    /// </summary>
    External
}
