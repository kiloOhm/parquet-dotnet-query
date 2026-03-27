namespace Parquet.Query.Extensions.Indexing;

internal sealed class FooterBitmapIndexModel
{
    public string ColumnPath { get; set; } = string.Empty;

    public List<FooterBitmapEntryModel> Values { get; set; } = [];
}

internal sealed class FooterBitmapEntryModel
{
    public string Value { get; set; } = string.Empty;

    public int[] RowGroups { get; set; } = [];
}
