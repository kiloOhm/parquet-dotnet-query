namespace Parquet.Query.Extensions.Indexing;

/// <summary>
/// Well-known names for built-in footer indexing strategies.
/// </summary>
public static class FooterIndexNames
{
    /// <summary>
    /// The strategy name for the footer hash index.
    /// </summary>
    public const string HashStrategyName = "footer-hash";
    /// <summary>
    /// The strategy name for the footer bitmap index.
    /// </summary>
    public const string BitmapStrategyName = "footer-bitmap";
    /// <summary>
    /// The strategy name for the footer sort order index.
    /// </summary>
    public const string SortOrderStrategyName = "footer-sort-order";

    internal const string HashMetadataPrefix = "parquet.query.index.hash.v1/";
    internal const string BitmapMetadataPrefix = "parquet.query.index.bitmap.v1/";
    internal const string SortOrderMetadataKey = "parquet.query.index.sortorder.v1";
}
