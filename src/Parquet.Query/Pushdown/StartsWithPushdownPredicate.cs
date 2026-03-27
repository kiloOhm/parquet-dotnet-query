namespace Parquet.Query.Pushdown;

public sealed class StartsWithPushdownPredicate<T> : PushdownPredicate<T>
{
    internal StartsWithPushdownPredicate(
        string memberPath,
        string columnPath,
        string prefix,
        string description,
        Func<T, bool> rowPredicate)
        : base(memberPath, columnPath, description, rowPredicate)
    {
        Prefix = prefix;
    }

    public string Prefix { get; }
}
