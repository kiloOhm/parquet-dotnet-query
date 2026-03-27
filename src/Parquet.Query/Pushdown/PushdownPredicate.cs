namespace Parquet.Query.Pushdown;

public abstract class PushdownPredicate<T>
{
    protected PushdownPredicate(
        string memberPath,
        string columnPath,
        string description,
        Func<T, bool> rowPredicate)
    {
        MemberPath = memberPath;
        ColumnPath = columnPath;
        Description = description;
        RowPredicate = rowPredicate;
    }

    public string MemberPath { get; }

    public string ColumnPath { get; }

    public string Description { get; }

    internal Func<T, bool> RowPredicate { get; }
}
