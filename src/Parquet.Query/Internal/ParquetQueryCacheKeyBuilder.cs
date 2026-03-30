using System.Runtime.CompilerServices;
using System.Text;
using System.Linq.Expressions;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Internal;

internal static class ParquetQueryCacheKeyBuilder
{
    public static string Build<TSource, TResult>(
        IReadOnlyList<string> filePaths,
        ParquetOptions? parquetOptions,
        PushdownFilter<TSource> pushdownFilter,
        IReadOnlyList<IParquetPredicatePlanner<TSource>> predicatePlanners,
        IReadOnlyList<Expression<Func<TSource, bool>>> wherePredicates,
        Expression<Func<TSource, TResult>>? projection,
        IParquetReaderFactory readerFactory,
        bool countOnly)
        where TSource : class, new()
    {
        var builder = new StringBuilder();
        builder.Append("v1|");
        builder.Append(typeof(TSource).AssemblyQualifiedName);
        builder.Append('|');
        builder.Append(typeof(TResult).AssemblyQualifiedName);
        builder.Append('|');
        builder.Append(countOnly ? "count" : "results");
        builder.Append('|');
        builder.Append(ParquetOptionsFingerprint.Create(parquetOptions));
        builder.Append('|');
        builder.Append(readerFactory.GetType().AssemblyQualifiedName);
        builder.Append(':');
        builder.Append(RuntimeHelpers.GetHashCode(readerFactory));
        builder.Append('|');

        foreach (var filePath in filePaths)
        {
            AppendFile(builder, filePath);
        }

        builder.Append('|');
        foreach (var predicate in pushdownFilter.Predicates)
        {
            builder.Append(predicate.Description);
            builder.Append(';');
        }

        builder.Append('|');
        foreach (var predicate in wherePredicates)
        {
            builder.Append(predicate);
            builder.Append(';');
        }

        builder.Append('|');
        builder.Append(projection);
        builder.Append('|');

        foreach (var planner in predicatePlanners)
        {
            builder.Append(planner.GetType().AssemblyQualifiedName);
            builder.Append(':');
            builder.Append(RuntimeHelpers.GetHashCode(planner));
            builder.Append(';');
        }

        return HashingCompatibility.Sha256Hex(builder.ToString());
    }

    private static void AppendFile(StringBuilder builder, string filePath)
    {
        builder.Append(filePath);
        builder.Append(':');

        if (!System.IO.File.Exists(filePath))
        {
            builder.Append("missing");
            builder.Append('|');
            return;
        }

        var fileInfo = new FileInfo(filePath);
        builder.Append(fileInfo.Length);
        builder.Append(':');
        builder.Append(fileInfo.LastWriteTimeUtc.Ticks);
        builder.Append('|');
    }
}
