namespace Parquet.Query.Dynamic;

/// <summary>
/// Internal sentinel type that satisfies the <c>class, new()</c> constraint
/// required by the generic query infrastructure, allowing dynamic (schema-less)
/// queries to flow through the same planning and pruning pipeline.
/// </summary>
internal sealed class DynamicRow
{
    public Dictionary<string, object?> Values { get; } = new(StringComparer.Ordinal);
}
