namespace Parquet.Query.Extensions.Search;

internal sealed class LuceneFooterIndexModel
{
    public string ColumnPath { get; set; } = string.Empty;

    public List<string> Terms { get; set; } = [];

    public List<int[]> RowGroups { get; set; } = [];
}
