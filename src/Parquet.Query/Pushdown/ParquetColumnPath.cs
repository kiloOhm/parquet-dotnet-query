using System.Linq.Expressions;
using Parquet.Query.Internal;

namespace Parquet.Query.Pushdown;

/// <summary>
/// Describes the mapping between a source member path and its parquet column path.
/// </summary>
public sealed class ParquetColumnPath
{
    /// <summary>
    /// Initializes a new column path mapping.
    /// </summary>
    /// <param name="memberPath">The source member path.</param>
    /// <param name="columnPath">The parquet column path.</param>
    public ParquetColumnPath(string memberPath, string columnPath)
    {
        MemberPath = memberPath ?? throw new ArgumentNullException(nameof(memberPath));
        ColumnPath = columnPath ?? throw new ArgumentNullException(nameof(columnPath));
    }

    /// <summary>
    /// Gets the source member path.
    /// </summary>
    public string MemberPath { get; }

    /// <summary>
    /// Gets the parquet column path.
    /// </summary>
    public string ColumnPath { get; }
}

/// <summary>
/// Resolves parquet column paths from member access expressions.
/// </summary>
public static class PushdownColumnPath
{
    /// <summary>
    /// Resolves the source member path and parquet column path referenced by an expression.
    /// </summary>
    /// <typeparam name="TSource">The source row type.</typeparam>
    /// <typeparam name="TValue">The selected member type.</typeparam>
    /// <param name="selector">The member selector to resolve.</param>
    /// <returns>The resolved path mapping.</returns>
    public static ParquetColumnPath Resolve<TSource, TValue>(Expression<Func<TSource, TValue>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        var path = ColumnPathResolver.FromLambda(selector);
        return new ParquetColumnPath(path.MemberPath, path.PhysicalPath);
    }
}
