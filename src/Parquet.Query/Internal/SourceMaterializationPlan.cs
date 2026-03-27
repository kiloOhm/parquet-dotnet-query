namespace Parquet.Query.Internal;

internal sealed class SourceMaterializationPlan<TSource>
    where TSource : class, new()
{
    public static SourceMaterializationPlan<TSource> Full { get; } = new(true, Array.Empty<SourceColumnBinding<TSource>>(), Array.Empty<string>());

    public SourceMaterializationPlan(
        bool requiresFullMaterialization,
        IReadOnlyList<SourceColumnBinding<TSource>> bindings,
        IReadOnlyList<string> requiredColumnPaths)
    {
        RequiresFullMaterialization = requiresFullMaterialization;
        Bindings = bindings;
        RequiredColumnPaths = requiredColumnPaths;
    }

    public bool RequiresFullMaterialization { get; }

    public IReadOnlyList<SourceColumnBinding<TSource>> Bindings { get; }

    public IReadOnlyList<string> RequiredColumnPaths { get; }
}

internal sealed class SourceColumnBinding<TSource>
    where TSource : class, new()
{
    public SourceColumnBinding(string memberPath, string columnPath, Action<TSource, object?> assign)
    {
        MemberPath = memberPath;
        ColumnPath = columnPath;
        Assign = assign;
    }

    public string MemberPath { get; }

    public string ColumnPath { get; }

    public Action<TSource, object?> Assign { get; }
}
