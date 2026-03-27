namespace Parquet.Query.Extensions.Indexing;

internal sealed class FooterHashIndexModel
{
    public string ColumnPath { get; set; } = string.Empty;

    public int BucketCount { get; set; }

    public List<FooterHashBucketModel> Buckets { get; set; } = [];
}

internal sealed class FooterHashBucketModel
{
    public int Bucket { get; set; }

    public int[] RowGroups { get; set; } = [];
}
