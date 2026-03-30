namespace Parquet.Query.Pushdown;

/// <summary>
/// Entry point helpers for building pushdown filters.
/// </summary>
public static class Pushdown
{
    /// <summary>
    /// Builds a pushdown filter by using a fluent builder callback.
    /// </summary>
    /// <typeparam name="T">The source row type the filter targets.</typeparam>
    /// <param name="configure">The callback that configures the filter builder.</param>
    /// <returns>The built pushdown filter.</returns>
    public static PushdownFilter<T> For<T>(Func<PushdownFilterBuilder<T>, PushdownFilterBuilder<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return configure(new PushdownFilterBuilder<T>()).Build();
    }
}
