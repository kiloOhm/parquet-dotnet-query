namespace Parquet.Query.Pushdown;

/// <summary>
/// Represents an ordinal string prefix predicate.
/// </summary>
/// <typeparam name="T">The source row type the predicate targets.</typeparam>
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

    /// <summary>
    /// Gets the required string prefix.
    /// </summary>
    public string Prefix { get; }
}
