using Parquet.Schema;
using Parquet.Serialization;

namespace Parquet.Query.Internal;

internal static class PartialRowMaterializer<TSource>
    where TSource : class, new()
{
    public static async Task<IReadOnlyList<TSource>> ReadRowGroupAsync(
        string filePath,
        ParquetReader reader,
        int rowGroupIndex,
        SourceMaterializationPlan<TSource> plan,
        ParquetSerializerOptions? serializerOptions,
        CancellationToken cancellationToken)
    {
        if (plan.RequiresFullMaterialization)
        {
            var deserializedRows = await ParquetSerializer.DeserializeAsync<TSource>(filePath, rowGroupIndex, serializerOptions, cancellationToken);
            return deserializedRows.ToArray();
        }

        var rows = await ReadFilterRowsAsync(reader, rowGroupIndex, plan, cancellationToken);
        if (plan.DeferredBindings.Count > 0)
        {
            var selectedIndexes = Enumerable.Range(0, rows.Length).ToArray();
            await PopulateDeferredColumnsAsync(reader, rowGroupIndex, plan, rows, selectedIndexes, cancellationToken);
        }

        return rows;
    }

    public static async Task<TSource[]> ReadFilterRowsAsync(
        ParquetReader reader,
        int rowGroupIndex,
        SourceMaterializationPlan<TSource> plan,
        CancellationToken cancellationToken)
    {
        if (plan.RequiresFullMaterialization)
        {
            throw new InvalidOperationException("Filter-only materialization is not available when full materialization is required.");
        }

        using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
        var rows = CreateRowBuffer(rowGroupReader.RowCount);
        await PopulateBindingsAsync(rowGroupReader, reader.Schema, rows, plan.FilterBindings, selectedIndexes: null, cancellationToken);
        return rows;
    }

    public static async Task PopulateDeferredColumnsAsync(
        ParquetReader reader,
        int rowGroupIndex,
        SourceMaterializationPlan<TSource> plan,
        IReadOnlyList<TSource> rows,
        IReadOnlyList<int> selectedIndexes,
        CancellationToken cancellationToken)
    {
        if (plan.RequiresFullMaterialization || plan.DeferredBindings.Count == 0 || selectedIndexes.Count == 0)
        {
            return;
        }

        using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
        await PopulateBindingsAsync(rowGroupReader, reader.Schema, rows, plan.DeferredBindings, selectedIndexes, cancellationToken);
    }

    private static async Task PopulateBindingsAsync(
        IParquetRowGroupReader rowGroupReader,
        ParquetSchema schema,
        IReadOnlyList<TSource> rows,
        IReadOnlyList<SourceColumnBinding<TSource>> bindings,
        IReadOnlyList<int>? selectedIndexes,
        CancellationToken cancellationToken)
    {
        if (bindings.Count == 0 || rows.Count == 0)
        {
            return;
        }

        var dataFields = schema.GetDataFields()
            .ToDictionary(field => field.Path.ToString(), StringComparer.Ordinal);

        foreach (var binding in bindings)
        {
            if (!dataFields.TryGetValue(binding.ColumnPath, out DataField? field))
            {
                continue;
            }

            var column = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
            var data = column.Data;
            var length = Math.Min(rows.Count, data.Length);

            if (selectedIndexes is null)
            {
                for (var index = 0; index < length; index++)
                {
                    binding.Assign(rows[index], data.GetValue(index));
                }

                continue;
            }

            foreach (var selectedIndex in selectedIndexes)
            {
                if ((uint)selectedIndex >= (uint)length)
                {
                    continue;
                }

                binding.Assign(rows[selectedIndex], data.GetValue(selectedIndex));
            }
        }
    }

    private static TSource[] CreateRowBuffer(long rowCount)
    {
        if (rowCount > int.MaxValue)
        {
            throw new NotSupportedException("Row groups larger than Int32.MaxValue are not supported by the partial materializer.");
        }

        return Enumerable.Range(0, (int)rowCount).Select(_ => new TSource()).ToArray();
    }
}
