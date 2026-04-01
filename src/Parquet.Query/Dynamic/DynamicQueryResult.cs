using Parquet.Query.Planning;

namespace Parquet.Query.Dynamic;

/// <summary>
/// Contains the results of executing a dynamic parquet query, including the query plan
/// and the matching rows for the requested page.
/// </summary>
public sealed class DynamicQueryResult
{
    internal DynamicQueryResult(
        ParquetQueryPlan plan,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> rows,
        int offset,
        int limit,
        long totalMatchedRows)
    {
        Plan = plan;
        Rows = rows;
        Offset = offset;
        Limit = limit;
        TotalMatchedRows = totalMatchedRows;
    }

    /// <summary>
    /// Gets the query plan that describes how the query was executed.
    /// </summary>
    public ParquetQueryPlan Plan { get; }

    /// <summary>
    /// Gets the matching rows for the requested page.
    /// </summary>
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows { get; }

    /// <summary>
    /// Gets the zero-based offset into the full result set.
    /// </summary>
    public int Offset { get; }

    /// <summary>
    /// Gets the maximum number of rows requested.
    /// </summary>
    public int Limit { get; }

    /// <summary>
    /// Gets the total number of rows that matched the query predicates.
    /// </summary>
    public long TotalMatchedRows { get; }
}
