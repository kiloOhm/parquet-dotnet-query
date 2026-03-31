namespace Parquet.Query.Extensions.Indexing;

internal sealed class FooterSortOrderModel
{
    public List<FooterSortOrderColumnModel> Columns { get; set; } = [];
}

internal sealed class FooterSortOrderColumnModel
{
    public string ColumnPath { get; set; } = string.Empty;

    public int Order { get; set; }

    public string Direction { get; set; } = "ascending";
}
