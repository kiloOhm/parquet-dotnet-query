using System.Linq;

namespace Parquet.Query.Internal;

internal sealed class SourceMaterializationPlan<TSource>
    where TSource : class, new()
{
    public static SourceMaterializationPlan<TSource> Full { get; } = new(
        requiresFullMaterialization: true,
        filterBindings: Array.Empty<SourceColumnBinding<TSource>>(),
        resultBindings: Array.Empty<SourceColumnBinding<TSource>>(),
        deferredBindings: Array.Empty<SourceColumnBinding<TSource>>(),
        filterColumnPaths: Array.Empty<string>(),
        resultColumnPaths: Array.Empty<string>(),
        deferredColumnPaths: Array.Empty<string>(),
        requiredColumnPaths: Array.Empty<string>());

    public SourceMaterializationPlan(
        bool requiresFullMaterialization,
        IReadOnlyList<SourceColumnBinding<TSource>> filterBindings,
        IReadOnlyList<SourceColumnBinding<TSource>> resultBindings,
        IReadOnlyList<SourceColumnBinding<TSource>> deferredBindings,
        IReadOnlyList<string> filterColumnPaths,
        IReadOnlyList<string> resultColumnPaths,
        IReadOnlyList<string> deferredColumnPaths,
        IReadOnlyList<string> requiredColumnPaths)
    {
        RequiresFullMaterialization = requiresFullMaterialization;
        FilterBindings = filterBindings;
        ResultBindings = resultBindings;
        DeferredBindings = deferredBindings;
        FilterColumnPaths = filterColumnPaths;
        ResultColumnPaths = resultColumnPaths;
        DeferredColumnPaths = deferredColumnPaths;
        RequiredColumnPaths = requiredColumnPaths;
    }

    public bool RequiresFullMaterialization { get; }

    public IReadOnlyList<SourceColumnBinding<TSource>> FilterBindings { get; }

    public IReadOnlyList<SourceColumnBinding<TSource>> ResultBindings { get; }

    public IReadOnlyList<SourceColumnBinding<TSource>> DeferredBindings { get; }

    public IReadOnlyList<string> FilterColumnPaths { get; }

    public IReadOnlyList<string> ResultColumnPaths { get; }

    public IReadOnlyList<string> DeferredColumnPaths { get; }

    public IReadOnlyList<string> RequiredColumnPaths { get; }

    public bool UsesLateMaterialization => !RequiresFullMaterialization && DeferredBindings.Count > 0;

    public SourceColumnBinding<TSource>? FindBinding(string memberPath) =>
        FilterBindings.Concat(ResultBindings)
            .FirstOrDefault(binding => string.Equals(binding.MemberPath, memberPath, StringComparison.Ordinal));
}

internal sealed class SourceColumnBinding<TSource>
    where TSource : class, new()
{
    public SourceColumnBinding(
        string memberPath,
        string columnPath,
        Action<TSource, object?> assign,
        Func<TSource, object?> read,
        bool requiresFullRowRead)
    {
        MemberPath = memberPath;
        ColumnPath = columnPath;
        Assign = assign;
        Read = read;
        RequiresFullRowRead = requiresFullRowRead;
    }

    public string MemberPath { get; }

    public string ColumnPath { get; }

    public Action<TSource, object?> Assign { get; }

    public Func<TSource, object?> Read { get; }

    public bool RequiresFullRowRead { get; }
}
