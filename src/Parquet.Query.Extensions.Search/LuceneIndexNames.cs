namespace Parquet.Query.Extensions.Search;

/// <summary>
/// Well-known names for Lucene-style indexing support.
/// </summary>
public static class LuceneIndexNames
{
    /// <summary>
    /// The strategy name for the Lucene footer index.
    /// </summary>
    public const string StrategyName = "lucene";
    internal const string MetadataPrefix = "parquet.query.lucene.v1/";
}
