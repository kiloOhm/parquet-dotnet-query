namespace Parquet.Query.Extensions.Indexing;

public static class FooterIndexNames
{
    public const string HashStrategyName = "footer-hash";
    public const string BitmapStrategyName = "footer-bitmap";

    internal const string HashMetadataPrefix = "parquet.query.index.hash.v1/";
    internal const string BitmapMetadataPrefix = "parquet.query.index.bitmap.v1/";
}
