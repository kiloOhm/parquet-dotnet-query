using Parquet;
using Parquet.Meta;
using Parquet.Query.Planning;
using Parquet.Schema;
using Parquet.Serialization;
using System.Linq;
using System.Reflection;

namespace Parquet.Query.Internal;

internal static class PartialRowMaterializer<TSource>
    where TSource : class, new()
{
    public static async Task<IReadOnlyList<TSource>> ReadRowGroupAsync(
        ParquetReader reader,
        int rowGroupIndex,
        SourceMaterializationPlan<TSource> plan,
        ParquetSerializerOptions? serializerOptions,
        IReadOnlyList<RowInterval>? candidateIntervals,
        CancellationToken cancellationToken)
    {
        if (plan.RequiresFullMaterialization)
        {
            return await DeserializeRowGroupAsync(reader, rowGroupIndex, serializerOptions, cancellationToken);
        }

        if (plan.DeferredBindings.Any(binding => binding.RequiresFullRowRead))
        {
            return await DeserializeRowGroupAsync(reader, rowGroupIndex, serializerOptions, cancellationToken);
        }

        var rowSet = await ReadFilterRowsAsync(reader, rowGroupIndex, plan, candidateIntervals, cancellationToken);
        if (plan.DeferredBindings.Count > 0)
        {
            var selectedIndexes = Enumerable.Range(0, rowSet.Rows.Length).ToArray();
            await PopulateDeferredColumnsAsync(reader, rowGroupIndex, plan, rowSet, selectedIndexes, cancellationToken);
        }

        return rowSet.Rows;
    }

    private static async Task<IReadOnlyList<TSource>> DeserializeRowGroupAsync(
        ParquetReader reader,
        int rowGroupIndex,
        ParquetSerializerOptions? serializerOptions,
        CancellationToken cancellationToken)
    {
        var rows = new List<TSource>();
        await ParquetSerializer.DeserializeAsync(
            reader,
            rowGroupIndex,
            rows,
            cancellationToken,
            resultsAlreadyAllocated: false,
            options: serializerOptions);
        return rows.ToArray();
    }

    public static async Task<MaterializedRowSet<TSource>> ReadFilterRowsAsync(
        ParquetReader reader,
        int rowGroupIndex,
        SourceMaterializationPlan<TSource> plan,
        IReadOnlyList<RowInterval>? candidateIntervals,
        CancellationToken cancellationToken)
    {
        if (plan.RequiresFullMaterialization)
        {
            throw new InvalidOperationException("Filter-only materialization is not available when full materialization is required.");
        }

        using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
        var rowIndexes = CreateRowIndexes(rowGroupReader.RowCount, candidateIntervals);
        var rows = CreateRowBuffer(rowIndexes.Length);
        await PopulateBindingsAsync(rowGroupReader, reader.Schema, rows, rowIndexes, plan.FilterBindings, cancellationToken);
        return new MaterializedRowSet<TSource>(rows, rowIndexes);
    }

    public static async Task PopulateDeferredColumnsAsync(
        ParquetReader reader,
        int rowGroupIndex,
        SourceMaterializationPlan<TSource> plan,
        MaterializedRowSet<TSource> rowSet,
        IReadOnlyList<int> selectedIndexes,
        CancellationToken cancellationToken)
    {
        if (plan.RequiresFullMaterialization || plan.DeferredBindings.Count == 0 || selectedIndexes.Count == 0)
        {
            return;
        }

        var selectedRowIndexes = new int[selectedIndexes.Count];
        var selectedRows = new TSource[selectedIndexes.Count];
        for (var index = 0; index < selectedIndexes.Count; index++)
        {
            var selectedIndex = selectedIndexes[index];
            selectedRowIndexes[index] = rowSet.RowIndexes[selectedIndex];
            selectedRows[index] = rowSet.Rows[selectedIndex];
        }

        using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
        await PopulateBindingsAsync(rowGroupReader, reader.Schema, selectedRows, selectedRowIndexes, plan.DeferredBindings, cancellationToken);
    }

    public static async Task<object?[]> ReadColumnValuesAsync(
        ParquetReader reader,
        int rowGroupIndex,
        string columnPath,
        IReadOnlyList<int> rowIndexes,
        CancellationToken cancellationToken)
    {
        using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
        return await ReadColumnValuesAsync(rowGroupReader, reader.Schema, columnPath, rowIndexes, cancellationToken);
    }

    public static async Task<object?[]> ReadColumnValuesAsync(
        IParquetRowGroupReader rowGroupReader,
        ParquetSchema schema,
        string columnPath,
        IReadOnlyList<int> rowIndexes,
        CancellationToken cancellationToken)
    {
        if (rowIndexes.Count == 0)
        {
            return Array.Empty<object?>();
        }

        var dataFields = schema.GetDataFields()
            .ToDictionary(field => field.Path.ToString(), StringComparer.Ordinal);
        if (!dataFields.TryGetValue(columnPath, out DataField? field))
        {
            return new object?[rowIndexes.Count];
        }

        if (field.IsArray)
        {
            var arrayColumn = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
            return CopyIndexedValues(rowIndexes, arrayColumn.Data);
        }

        var values = new object?[rowIndexes.Count];
        var fullCoverage = rowIndexes.Count == rowGroupReader.RowCount && IsIdentityMap(rowIndexes);
        if (fullCoverage)
        {
            var denseColumn = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
            CopyDenseValues(values, denseColumn.Data);
            return values;
        }

        var rowIntervals = PagePruner.ToIntervals(rowIndexes);
        if (rowIntervals.Count == 0)
        {
            return values;
        }

        var pages = await TryReadSparsePagesAsync(rowGroupReader, field, rowIntervals, cancellationToken).ConfigureAwait(false);
        if (pages is null)
        {
            var selectedColumn = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
            CopyIndexedValues(values, rowIndexes, selectedColumn.Data);
            return values;
        }

        CopySparseValues(values, rowIndexes, pages);
        return values;
    }

    private static async Task PopulateBindingsAsync(
        IParquetRowGroupReader rowGroupReader,
        ParquetSchema schema,
        IReadOnlyList<TSource> rows,
        IReadOnlyList<int> rowIndexes,
        IReadOnlyList<SourceColumnBinding<TSource>> bindings,
        CancellationToken cancellationToken)
    {
        if (bindings.Count == 0 || rows.Count == 0)
        {
            return;
        }

        var dataFields = schema.GetDataFields()
            .ToDictionary(field => field.Path.ToString(), StringComparer.Ordinal);
        var fullCoverage = rowIndexes.Count == rows.Count &&
            rowIndexes.Count == rowGroupReader.RowCount &&
            IsIdentityMap(rowIndexes);
        var rowIntervals = fullCoverage ? null : PagePruner.ToIntervals(rowIndexes);

        foreach (var binding in bindings)
        {
            if (!dataFields.TryGetValue(binding.ColumnPath, out DataField? field))
            {
                continue;
            }

            if (field.IsArray)
            {
                var arrayColumn = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
                AssignIndexedValues(binding, rows, rowIndexes, arrayColumn.Data);
                continue;
            }

            if (fullCoverage)
            {
                var denseColumn = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
                AssignDenseValues(binding, rows, denseColumn.Data);
                continue;
            }

            if (rowIntervals is null || rowIntervals.Count == 0)
            {
                continue;
            }

            var pages = await TryReadSparsePagesAsync(rowGroupReader, field, rowIntervals, cancellationToken).ConfigureAwait(false);
            if (pages is null)
            {
                var selectedColumn = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
                AssignIndexedValues(binding, rows, rowIndexes, selectedColumn.Data);
                continue;
            }

            AssignSparseValues(binding, rows, rowIndexes, pages);
        }
    }

    private static void AssignDenseValues(
        SourceColumnBinding<TSource> binding,
        IReadOnlyList<TSource> rows,
        Array data)
    {
        var length = Math.Min(rows.Count, data.Length);
        for (var index = 0; index < length; index++)
        {
            binding.Assign(rows[index], data.GetValue(index));
        }
    }

    private static void AssignIndexedValues(
        SourceColumnBinding<TSource> binding,
        IReadOnlyList<TSource> rows,
        IReadOnlyList<int> rowIndexes,
        Array data)
    {
        var length = Math.Min(rows.Count, rowIndexes.Count);
        for (var index = 0; index < length; index++)
        {
            var sourceIndex = rowIndexes[index];
            if ((uint)sourceIndex < (uint)data.Length)
            {
                binding.Assign(rows[index], data.GetValue(sourceIndex));
            }
        }
    }

    private static void CopyDenseValues(object?[] target, Array data)
    {
        var length = Math.Min(target.Length, data.Length);
        for (var index = 0; index < length; index++)
        {
            target[index] = data.GetValue(index);
        }
    }

    private static object?[] CopyIndexedValues(IReadOnlyList<int> rowIndexes, Array data)
    {
        var values = new object?[rowIndexes.Count];
        for (var index = 0; index < rowIndexes.Count; index++)
        {
            var sourceIndex = rowIndexes[index];
            if ((uint)sourceIndex < (uint)data.Length)
            {
                values[index] = data.GetValue(sourceIndex);
            }
        }

        return values;
    }

    private static void AssignSparseValues(
        SourceColumnBinding<TSource> binding,
        IReadOnlyList<TSource> rows,
        IReadOnlyList<int> rowIndexes,
        IReadOnlyList<ParquetDataPage> pages)
    {
        var denseIndex = 0;
        foreach (var page in pages)
        {
            var pageStart = page.Location.FirstRowIndex;
            var pageEnd = pageStart + page.RowCount;
            while (denseIndex < rowIndexes.Count && rowIndexes[denseIndex] < pageStart)
            {
                denseIndex++;
            }

            var data = page.Column.Data;
            var pageDenseIndex = denseIndex;
            while (pageDenseIndex < rowIndexes.Count && rowIndexes[pageDenseIndex] < pageEnd)
            {
                var pageRowIndex = checked((int)(rowIndexes[pageDenseIndex] - pageStart));
                if ((uint)pageRowIndex < (uint)data.Length)
                {
                    binding.Assign(rows[pageDenseIndex], data.GetValue(pageRowIndex));
                }

                pageDenseIndex++;
            }

            denseIndex = pageDenseIndex;
            if (denseIndex >= rowIndexes.Count)
            {
                break;
            }
        }
    }

    private static void CopyIndexedValues(object?[] target, IReadOnlyList<int> rowIndexes, Array data)
    {
        var length = Math.Min(target.Length, rowIndexes.Count);
        for (var index = 0; index < length; index++)
        {
            var sourceIndex = rowIndexes[index];
            if ((uint)sourceIndex < (uint)data.Length)
            {
                target[index] = data.GetValue(sourceIndex);
            }
        }
    }

    private static void CopySparseValues(
        object?[] target,
        IReadOnlyList<int> rowIndexes,
        IReadOnlyList<ParquetDataPage> pages)
    {
        var denseIndex = 0;
        foreach (var page in pages)
        {
            var pageStart = page.Location.FirstRowIndex;
            var pageEnd = pageStart + page.RowCount;
            while (denseIndex < rowIndexes.Count && rowIndexes[denseIndex] < pageStart)
            {
                denseIndex++;
            }

            var data = page.Column.Data;
            var pageDenseIndex = denseIndex;
            while (pageDenseIndex < rowIndexes.Count && rowIndexes[pageDenseIndex] < pageEnd)
            {
                var pageRowIndex = checked((int)(rowIndexes[pageDenseIndex] - pageStart));
                if ((uint)pageRowIndex < (uint)data.Length)
                {
                    target[pageDenseIndex] = data.GetValue(pageRowIndex);
                }

                pageDenseIndex++;
            }

            denseIndex = pageDenseIndex;
            if (denseIndex >= rowIndexes.Count)
            {
                break;
            }
        }
    }

    private static async Task<IReadOnlyList<ParquetDataPage>?> TryReadSparsePagesAsync(
        IParquetRowGroupReader rowGroupReader,
        DataField field,
        IReadOnlyList<RowInterval> rowIntervals,
        CancellationToken cancellationToken)
    {
        object? pageReader =
#if NET48
            await OpenColumnPageReaderCompatAsync(rowGroupReader, field, cancellationToken).ConfigureAwait(false);
#else
            await rowGroupReader.OpenColumnPageReaderAsync(field, cancellationToken).ConfigureAwait(false);
#endif
        if (pageReader is null)
        {
            return null;
        }

        var offsetIndex = GetOffsetIndex(pageReader);
        if (offsetIndex is null)
        {
            return null;
        }

        var pageOrdinals = PagePruner.SelectPageOrdinals(offsetIndex, rowIntervals, rowGroupReader.RowCount);
        if (pageOrdinals.Count == 0)
        {
            return Array.Empty<ParquetDataPage>();
        }

#if NET48
        return await ReadPagesCompatAsync(pageReader, pageOrdinals, cancellationToken).ConfigureAwait(false);
#else
        return await ((dynamic)pageReader).ReadPagesAsync(pageOrdinals, cancellationToken).ConfigureAwait(false);
#endif
    }

#if NET48
    private static async Task<object?> OpenColumnPageReaderCompatAsync(
        IParquetRowGroupReader rowGroupReader,
        DataField field,
        CancellationToken cancellationToken)
    {
        var openMethod = rowGroupReader.GetType().GetMethod(
            "OpenColumnPageReaderAsync",
            BindingFlags.Instance | BindingFlags.Public,
            binder: null,
            types: new[] { typeof(DataField), typeof(CancellationToken) },
            modifiers: null);
        if (openMethod is null)
        {
            return null;
        }

        return await AwaitTaskResultAsync(openMethod.Invoke(rowGroupReader, new object?[] { field, cancellationToken })).ConfigureAwait(false);
    }

    private static async Task<IReadOnlyList<ParquetDataPage>?> ReadPagesCompatAsync(
        object pageReader,
        IReadOnlyList<int> pageOrdinals,
        CancellationToken cancellationToken)
    {
        var readMethod = pageReader.GetType().GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(method =>
            {
                if (method.Name != "ReadPagesAsync")
                {
                    return false;
                }

                var parameters = method.GetParameters();
                return parameters.Length == 2 &&
                    parameters[1].ParameterType == typeof(CancellationToken);
            });
        if (readMethod is null)
        {
            return null;
        }

        var result = await AwaitTaskResultAsync(readMethod.Invoke(pageReader, new object?[] { pageOrdinals, cancellationToken })).ConfigureAwait(false);
        if (result is IReadOnlyList<ParquetDataPage> pages)
        {
            return pages;
        }

        return result is IEnumerable<ParquetDataPage> enumerable
            ? enumerable.ToArray()
            : null;
    }

    private static async Task<object?> AwaitTaskResultAsync(object? taskLike)
    {
        if (taskLike is not Task task)
        {
            return null;
        }

        await task.ConfigureAwait(false);
        return taskLike.GetType().GetProperty("Result", BindingFlags.Instance | BindingFlags.Public)?.GetValue(taskLike);
    }
#endif

    private static OffsetIndex? GetOffsetIndex(object pageReader) =>
        pageReader.GetType().GetProperty("OffsetIndex", BindingFlags.Instance | BindingFlags.Public)?.GetValue(pageReader) as OffsetIndex;

    private static int[] CreateRowIndexes(long rowGroupRowCount, IReadOnlyList<RowInterval>? candidateIntervals)
    {
        if (rowGroupRowCount > int.MaxValue)
        {
            throw new NotSupportedException("Row groups larger than Int32.MaxValue are not supported by the partial materializer.");
        }

        if (candidateIntervals is null || candidateIntervals.Count == 0)
        {
            return Enumerable.Range(0, (int)rowGroupRowCount).ToArray();
        }

        return PagePruner.ExpandRowIndexes(candidateIntervals, (int)rowGroupRowCount).ToArray();
    }

    private static bool IsIdentityMap(IReadOnlyList<int> rowIndexes)
    {
        for (var index = 0; index < rowIndexes.Count; index++)
        {
            if (rowIndexes[index] != index)
            {
                return false;
            }
        }

        return true;
    }

    private static TSource[] CreateRowBuffer(int rowCount)
    {
        var rows = new TSource[rowCount];
        for (var index = 0; index < rowCount; index++)
        {
            rows[index] = new TSource();
        }

        return rows;
    }
}

internal sealed class MaterializedRowSet<TSource>
    where TSource : class, new()
{
    public MaterializedRowSet(TSource[] rows, int[] rowIndexes)
    {
        Rows = rows;
        RowIndexes = rowIndexes;
    }

    public TSource[] Rows { get; }

    public int[] RowIndexes { get; }
}
