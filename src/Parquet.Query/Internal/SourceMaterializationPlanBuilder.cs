using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;
using Parquet.Query.Pushdown;
using Parquet.Schema;

namespace Parquet.Query.Internal;

internal static class SourceMaterializationPlanBuilder
{
    public static SourceMaterializationPlan<TSource> Build<TSource, TResult>(
        ParquetSchema schema,
        PushdownFilter<TSource> pushdownFilter,
        IReadOnlyList<Expression<Func<TSource, bool>>> wherePredicates,
        LambdaExpression? projection)
        where TSource : class, new()
    {
        ArgumentNullException.ThrowIfNull(schema);

        HashSet<string> resultMemberPaths;
        if (projection is null)
        {
            if (!TryGetDefaultResultMemberPaths(typeof(TSource), out resultMemberPaths))
            {
                return SourceMaterializationPlan<TSource>.Full;
            }
        }
        else
        {
            var projectionAnalysis = SourceAccessAnalyzer.Analyze(projection);
            if (projectionAnalysis.RequiresFullMaterialization)
            {
                return SourceMaterializationPlan<TSource>.Full;
            }

            resultMemberPaths = new HashSet<string>(StringComparer.Ordinal);
            foreach (var memberPath in projectionAnalysis.MemberPaths)
            {
                foreach (var expandedPath in ExpandProjectionPath(typeof(TSource), memberPath))
                {
                    resultMemberPaths.Add(expandedPath);
                }
            }
        }

        var filterMemberPaths = new HashSet<string>(
            pushdownFilter.Predicates.Select(predicate => predicate.MemberPath),
            StringComparer.Ordinal);

        foreach (var predicate in wherePredicates)
        {
            var analysis = SourceAccessAnalyzer.Analyze(predicate);
            if (analysis.RequiresFullMaterialization)
            {
                return SourceMaterializationPlan<TSource>.Full;
            }

            foreach (var memberPath in analysis.MemberPaths)
            {
                if (!CanMaterializeForPredicate(typeof(TSource), memberPath))
                {
                    return SourceMaterializationPlan<TSource>.Full;
                }

                filterMemberPaths.Add(memberPath);
            }
        }

        var filterBindings = CreateBindings<TSource>(filterMemberPaths);
        var resultBindings = CreateBindings<TSource>(resultMemberPaths);
        if (filterBindings is null || resultBindings is null)
        {
            return SourceMaterializationPlan<TSource>.Full;
        }

        var deferredBindings = resultBindings
            .Where(binding => filterBindings.All(filterBinding => !string.Equals(filterBinding.MemberPath, binding.MemberPath, StringComparison.Ordinal)))
            .ToArray();

        var requiredColumnPaths = filterBindings
            .Concat(resultBindings)
            .Select(binding => binding.ColumnPath)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToArray();

        return new SourceMaterializationPlan<TSource>(
            requiresFullMaterialization: false,
            new ReadOnlyCollection<SourceColumnBinding<TSource>>(filterBindings),
            new ReadOnlyCollection<SourceColumnBinding<TSource>>(resultBindings),
            new ReadOnlyCollection<SourceColumnBinding<TSource>>(deferredBindings),
            new ReadOnlyCollection<string>(filterBindings.Select(binding => binding.ColumnPath).Distinct(StringComparer.Ordinal).OrderBy(path => path, StringComparer.Ordinal).ToArray()),
            new ReadOnlyCollection<string>(resultBindings.Select(binding => binding.ColumnPath).Distinct(StringComparer.Ordinal).OrderBy(path => path, StringComparer.Ordinal).ToArray()),
            new ReadOnlyCollection<string>(deferredBindings.Select(binding => binding.ColumnPath).Distinct(StringComparer.Ordinal).OrderBy(path => path, StringComparer.Ordinal).ToArray()),
            new ReadOnlyCollection<string>(requiredColumnPaths));
    }

    private static bool TryGetDefaultResultMemberPaths(Type rootType, out HashSet<string> memberPaths)
    {
        memberPaths = new HashSet<string>(StringComparer.Ordinal);
        foreach (var member in GetReadableMembers(rootType))
        {
            if (!TryExpandReadablePath(GetMemberType(member), member.Name, memberPaths, new HashSet<Type> { rootType }))
            {
                memberPaths.Clear();
                return false;
            }
        }

        return memberPaths.Count > 0;
    }

    private static bool TryExpandReadablePath(
        Type memberType,
        string memberPath,
        HashSet<string> output,
        HashSet<Type> ancestry)
    {
        if (IsScalarLike(memberType))
        {
            output.Add(memberPath);
            return true;
        }

        if (!CanTraverseComplexType(memberType))
        {
            return false;
        }

        var underlyingType = Nullable.GetUnderlyingType(memberType) ?? memberType;
        if (!ancestry.Add(underlyingType))
        {
            return false;
        }

        foreach (var childMember in GetReadableMembers(underlyingType))
        {
            if (!TryExpandReadablePath(GetMemberType(childMember), $"{memberPath}.{childMember.Name}", output, ancestry))
            {
                ancestry.Remove(underlyingType);
                return false;
            }
        }

        ancestry.Remove(underlyingType);
        return true;
    }

    private static SourceColumnBinding<TSource>[]? CreateBindings<TSource>(IEnumerable<string> memberPaths)
        where TSource : class, new()
    {
        var bindings = new List<SourceColumnBinding<TSource>>();
        foreach (var memberPath in memberPaths.OrderBy(path => path, StringComparer.Ordinal))
        {
            if (!TryCreateBinding<TSource>(memberPath, out SourceColumnBinding<TSource>? binding))
            {
                return null;
            }

            bindings.Add(binding!);
        }

        return bindings.ToArray();
    }

    private static bool CanMaterializeForPredicate(Type rootType, string memberPath)
    {
        if (!TryResolveChain(rootType, memberPath, out var chain))
        {
            return false;
        }

        var leafType = GetMemberType(chain[^1]);
        return IsScalarLike(leafType);
    }

    private static IReadOnlyList<string> ExpandProjectionPath(Type rootType, string memberPath)
    {
        if (!TryResolveChain(rootType, memberPath, out var chain))
        {
            throw new NotSupportedException($"Projection member path '{memberPath}' could not be resolved on '{rootType.Name}'.");
        }

        var leafType = GetMemberType(chain[^1]);
        if (IsScalarLike(leafType))
        {
            return new[] { memberPath };
        }

        if (!CanTraverseComplexType(leafType))
        {
            throw new NotSupportedException(
                $"Projection member path '{memberPath}' targets '{leafType.Name}', which cannot be partially materialized.");
        }

        var expanded = new List<string>();
        ExpandComplexPath(leafType, memberPath, expanded);
        return expanded;
    }

    private static void ExpandComplexPath(Type type, string prefix, List<string> output)
    {
        foreach (var member in GetReadableMembers(type))
        {
            var childPath = $"{prefix}.{member.Name}";
            var childType = GetMemberType(member);
            if (IsScalarLike(childType))
            {
                output.Add(childPath);
                continue;
            }

            if (!CanTraverseComplexType(childType))
            {
                throw new NotSupportedException(
                    $"Projection member path '{childPath}' targets '{childType.Name}', which cannot be partially materialized.");
            }

            ExpandComplexPath(childType, childPath, output);
        }
    }

    private static bool TryCreateBinding<TSource>(string memberPath, out SourceColumnBinding<TSource>? binding)
        where TSource : class, new()
    {
        binding = null;

        if (!TryResolveChain(typeof(TSource), memberPath, out var chain))
        {
            return false;
        }

        if (!CanAssignChain(chain))
        {
            return false;
        }

        var columnPath = string.Join("/", chain.Select(GetColumnName));
        binding = new SourceColumnBinding<TSource>(memberPath, columnPath, BuildAssigner<TSource>(chain));
        return true;
    }

    private static Action<TSource, object?> BuildAssigner<TSource>(IReadOnlyList<MemberInfo> chain)
        where TSource : class, new()
    {
        return (row, value) =>
        {
            object current = row;
            for (var index = 0; index < chain.Count - 1; index++)
            {
                var member = chain[index];
                var memberType = GetMemberType(member);
                var next = GetValue(current, member);
                if (next is null)
                {
                    next = Activator.CreateInstance(memberType)
                        ?? throw new InvalidOperationException($"Could not create an instance of '{memberType.Name}'.");
                    SetValue(current, member, next);
                }

                current = next;
            }

            var leaf = chain[^1];
            SetValue(current, leaf, value);
        };
    }

    private static bool CanAssignChain(IReadOnlyList<MemberInfo> chain)
    {
        for (var index = 0; index < chain.Count; index++)
        {
            var member = chain[index];
            var memberType = GetMemberType(member);
            var isLeaf = index == chain.Count - 1;

            if (member is PropertyInfo property && !property.CanWrite)
            {
                return false;
            }

            if (!isLeaf)
            {
                if (!CanTraverseComplexType(memberType))
                {
                    return false;
                }

                if (memberType.GetConstructor(Type.EmptyTypes) is null)
                {
                    return false;
                }
            }
            else if (!IsScalarLike(memberType))
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryResolveChain(Type rootType, string memberPath, out IReadOnlyList<MemberInfo> chain)
    {
        var members = new List<MemberInfo>();
        var currentType = rootType;
        foreach (var segment in memberPath.Split('.', StringSplitOptions.RemoveEmptyEntries))
        {
            var member = GetReadableMembers(currentType)
                .FirstOrDefault(candidate => string.Equals(candidate.Name, segment, StringComparison.Ordinal));
            if (member is null)
            {
                chain = Array.Empty<MemberInfo>();
                return false;
            }

            members.Add(member);
            currentType = GetMemberType(member);
        }

        chain = members;
        return members.Count > 0;
    }

    private static IEnumerable<MemberInfo> GetReadableMembers(Type type)
    {
        foreach (var property in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (property.CanRead && property.GetIndexParameters().Length == 0)
            {
                yield return property;
            }
        }

        foreach (var field in type.GetFields(BindingFlags.Instance | BindingFlags.Public))
        {
            yield return field;
        }
    }

    private static string GetColumnName(MemberInfo member) =>
        member.GetCustomAttribute<System.Text.Json.Serialization.JsonPropertyNameAttribute>()?.Name ?? member.Name;

    private static Type GetMemberType(MemberInfo member) =>
        member switch
        {
            PropertyInfo property => property.PropertyType,
            FieldInfo field => field.FieldType,
            _ => throw new NotSupportedException($"Unsupported member type '{member.MemberType}'.")
        };

    private static object? GetValue(object instance, MemberInfo member) =>
        member switch
        {
            PropertyInfo property => property.GetValue(instance),
            FieldInfo field => field.GetValue(instance),
            _ => throw new NotSupportedException($"Unsupported member type '{member.MemberType}'.")
        };

    private static void SetValue(object instance, MemberInfo member, object? value)
    {
        switch (member)
        {
            case PropertyInfo property:
                if (value is null && property.PropertyType.IsValueType && Nullable.GetUnderlyingType(property.PropertyType) is null)
                {
                    return;
                }

                property.SetValue(instance, value);
                return;

            case FieldInfo field:
                if (value is null && field.FieldType.IsValueType && Nullable.GetUnderlyingType(field.FieldType) is null)
                {
                    return;
                }

                field.SetValue(instance, value);
                return;

            default:
                throw new NotSupportedException($"Unsupported member type '{member.MemberType}'.");
        }
    }

    private static bool CanTraverseComplexType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        return underlyingType.IsClass && underlyingType != typeof(string);
    }

    private static bool IsScalarLike(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        if (underlyingType.IsEnum)
        {
            return true;
        }

        return underlyingType.IsPrimitive ||
            underlyingType == typeof(string) ||
            underlyingType == typeof(decimal) ||
            underlyingType == typeof(DateTime) ||
            underlyingType == typeof(DateTimeOffset) ||
            underlyingType == typeof(TimeSpan) ||
#if NET6_0_OR_GREATER
            underlyingType == typeof(DateOnly) ||
            underlyingType == typeof(TimeOnly) ||
#endif
            underlyingType == typeof(Guid) ||
            underlyingType == typeof(byte[]);
    }
}
