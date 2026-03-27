namespace Parquet.Query.Pushdown;

public static class Pushdown
{
    public static PushdownFilter<T> For<T>(Func<PushdownFilterBuilder<T>, PushdownFilterBuilder<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return configure(new PushdownFilterBuilder<T>()).Build();
    }
}
