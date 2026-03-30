namespace Parquet.Query.Compatibility;

internal static class ValueTaskCompatibility
{
    public static ValueTask CompletedTask
    {
        get
        {
#if NET6_0_OR_GREATER
            return ValueTask.CompletedTask;
#else
            return default;
#endif
        }
    }

    public static ValueTask<T> FromResult<T>(T value)
    {
#if NET6_0_OR_GREATER
        return ValueTask.FromResult(value);
#else
        return new ValueTask<T>(value);
#endif
    }
}
