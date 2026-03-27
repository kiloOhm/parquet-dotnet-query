using System.Linq.Expressions;
using Parquet.Query.Internal;

namespace Parquet.Query.Pushdown;

public sealed class ParquetColumnPath
{
    public ParquetColumnPath(string memberPath, string columnPath)
    {
        MemberPath = memberPath ?? throw new ArgumentNullException(nameof(memberPath));
        ColumnPath = columnPath ?? throw new ArgumentNullException(nameof(columnPath));
    }

    public string MemberPath { get; }

    public string ColumnPath { get; }
}

public static class PushdownColumnPath
{
    public static ParquetColumnPath Resolve<TSource, TValue>(Expression<Func<TSource, TValue>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        var path = ColumnPathResolver.FromLambda(selector);
        return new ParquetColumnPath(path.MemberPath, path.PhysicalPath);
    }
}
