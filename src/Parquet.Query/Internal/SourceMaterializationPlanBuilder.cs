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
        => Build(schema, pushdownFilter, wherePredicates, projection, includeDefaultResultPaths: true);

    public static SourceMaterializationPlan<TSource> Build<TSource>(
        ParquetSchema schema,
        PushdownFilter<TSource> pushdownFilter,
        IReadOnlyList<Expression<Func<TSource, bool>>> wherePredicates,
        LambdaExpression? projection,
        bool includeDefaultResultPaths)
        where TSource : class, new()
    {
        Guard.NotNull(schema, nameof(schema));

        HashSet<string> resultMemberPaths;
        if (projection is null)
        {
            resultMemberPaths = new HashSet<string>(StringComparer.Ordinal);
            if (includeDefaultResultPaths &&
                !TryGetDefaultResultMemberPaths(typeof(TSource), out resultMemberPaths))
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

        var filterBindings = CreateBindings<TSource>(schema, filterMemberPaths);
        var resultBindings = CreateBindings<TSource>(schema, resultMemberPaths);
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
        if (IsMaterializableLeaf(memberType))
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

    private static SourceColumnBinding<TSource>[]? CreateBindings<TSource>(ParquetSchema schema, IEnumerable<string> memberPaths)
        where TSource : class, new()
    {
        var bindings = new List<SourceColumnBinding<TSource>>();
        foreach (var memberPath in memberPaths.OrderBy(path => path, StringComparer.Ordinal))
        {
            if (!TryCreateBinding<TSource>(schema, memberPath, out SourceColumnBinding<TSource>? binding))
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

        var leafType = GetMemberType(chain[chain.Count - 1]);
        return IsPredicateLeaf(leafType);
    }

    private static IReadOnlyList<string> ExpandProjectionPath(Type rootType, string memberPath)
    {
        if (!TryResolveChain(rootType, memberPath, out var chain))
        {
            throw new NotSupportedException($"Projection member path '{memberPath}' could not be resolved on '{rootType.Name}'.");
        }

        var leafType = GetMemberType(chain[chain.Count - 1]);
        if (IsMaterializableLeaf(leafType))
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
            if (IsMaterializableLeaf(childType))
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

    private static bool TryCreateBinding<TSource>(ParquetSchema schema, string memberPath, out SourceColumnBinding<TSource>? binding)
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

        if (!TryResolveColumnPath(schema, chain, out string? columnPath))
        {
            return false;
        }

        binding = new SourceColumnBinding<TSource>(
            memberPath,
            columnPath!,
            BuildAssigner<TSource>(chain),
            BuildReader<TSource>(chain),
            requiresFullRowRead: TryGetCollectionElementType(GetMemberType(chain[chain.Count - 1]), out _));
        return true;
    }

    private static bool TryResolveColumnPath(ParquetSchema schema, IReadOnlyList<MemberInfo> chain, out string? columnPath)
    {
        Field? current = schema.Fields
            .FirstOrDefault(field => string.Equals(field.Name, GetColumnName(chain[0]), StringComparison.Ordinal));
        if (current is null)
        {
            columnPath = null;
            return false;
        }

        for (var index = 1; index < chain.Count; index++)
        {
            if (current is not StructField structField)
            {
                columnPath = null;
                return false;
            }

            current = structField.Fields
                .FirstOrDefault(field => string.Equals(field.Name, GetColumnName(chain[index]), StringComparison.Ordinal));
            if (current is null)
            {
                columnPath = null;
                return false;
            }
        }

        current = current switch
        {
            DataField dataField => dataField,
            ListField listField when listField.Item is DataField itemDataField => itemDataField,
            _ => null
        };

        if (current is not DataField resolvedDataField)
        {
            columnPath = null;
            return false;
        }

        columnPath = resolvedDataField.Path.ToString();
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

            var leaf = chain[chain.Count - 1];
            SetValue(current, leaf, value);
        };
    }

    private static Func<TSource, object?> BuildReader<TSource>(IReadOnlyList<MemberInfo> chain)
        where TSource : class, new()
    {
        return row =>
        {
            object? current = row;
            foreach (var member in chain)
            {
                if (current is null)
                {
                    return null;
                }

                current = GetValue(current, member);
            }

            return current;
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
            else if (!IsMaterializableLeaf(memberType))
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
        foreach (var segment in memberPath.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries))
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

                property.SetValue(instance, ConvertAssignedValue(value, property.PropertyType));
                return;

            case FieldInfo field:
                if (value is null && field.FieldType.IsValueType && Nullable.GetUnderlyingType(field.FieldType) is null)
                {
                    return;
                }

                field.SetValue(instance, ConvertAssignedValue(value, field.FieldType));
                return;

            default:
                throw new NotSupportedException($"Unsupported member type '{member.MemberType}'.");
        }
    }

    private static bool CanTraverseComplexType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        return underlyingType.IsClass &&
            !IsMaterializableLeaf(underlyingType);
    }

    private static bool IsPredicateLeaf(Type type)
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

    private static bool IsMaterializableLeaf(Type type)
    {
        if (IsPredicateLeaf(type))
        {
            return true;
        }

        return TryGetCollectionElementType(type, out Type? elementType) &&
            elementType is not null &&
            IsPredicateLeaf(elementType);
    }

    private static bool TryGetCollectionElementType(Type type, out Type? elementType)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        if (underlyingType == typeof(string) || underlyingType == typeof(byte[]))
        {
            elementType = null;
            return false;
        }

        if (underlyingType.IsArray)
        {
            elementType = underlyingType.GetElementType();
            return elementType is not null;
        }

        var enumerableType = underlyingType.IsGenericType && underlyingType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
            ? underlyingType
            : underlyingType.GetInterfaces()
                .FirstOrDefault(candidate => candidate.IsGenericType && candidate.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        if (enumerableType is not null)
        {
            elementType = enumerableType.GetGenericArguments()[0];
            return true;
        }

        elementType = null;
        return false;
    }

    private static object? ConvertAssignedValue(object? value, Type targetType)
    {
        if (value is null)
        {
            return null;
        }

        var nonNullableType = Nullable.GetUnderlyingType(targetType) ?? targetType;
        if (nonNullableType.IsInstanceOfType(value))
        {
            return value;
        }

        if (TryGetCollectionElementType(nonNullableType, out Type? elementType) && elementType is not null)
        {
            return ConvertCollectionValue(value, nonNullableType, elementType);
        }

        return PushdownPredicateFactory.ConvertValue(value, nonNullableType);
    }

    private static object ConvertCollectionValue(object value, Type targetType, Type elementType)
    {
        if (value is not System.Collections.IEnumerable enumerable || value is string or byte[])
        {
            return value;
        }

        var convertedItems = new List<object?>();
        foreach (var item in enumerable)
        {
            convertedItems.Add(item is null ? null : PushdownPredicateFactory.ConvertValue(item, elementType));
        }

        if (targetType.IsArray)
        {
            var array = Array.CreateInstance(elementType, convertedItems.Count);
            for (var index = 0; index < convertedItems.Count; index++)
            {
                array.SetValue(convertedItems[index], index);
            }

            return array;
        }

        var concreteListType = targetType.IsInterface || targetType.IsAbstract
            ? typeof(List<>).MakeGenericType(elementType)
            : targetType;
        var list = Activator.CreateInstance(concreteListType)
            ?? throw new InvalidOperationException($"Could not create an instance of '{concreteListType.Name}'.");
        var addMethod = concreteListType.GetMethod("Add", new[] { elementType });
        if (addMethod is not null)
        {
            foreach (var item in convertedItems)
            {
                addMethod.Invoke(list, new[] { item });
            }

            return list;
        }

        return convertedItems
            .Select(item => item is null ? null : PushdownPredicateFactory.ConvertValue(item, elementType))
            .ToList();
    }
}
