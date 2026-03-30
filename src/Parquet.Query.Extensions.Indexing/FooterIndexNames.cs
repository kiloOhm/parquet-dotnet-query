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

    internal const string HashMetadataPrefix = "parquet.query.index.hash.v1/";
    internal const string BitmapMetadataPrefix = "parquet.query.index.bitmap.v1/";
}
