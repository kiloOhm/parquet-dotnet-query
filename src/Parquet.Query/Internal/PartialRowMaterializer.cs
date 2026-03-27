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

        using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
        if (rowGroupReader.RowCount > int.MaxValue)
        {
            throw new NotSupportedException("Row groups larger than Int32.MaxValue are not supported by the partial materializer.");
        }

        var rowCount = (int)rowGroupReader.RowCount;
        var rows = Enumerable.Range(0, rowCount).Select(_ => new TSource()).ToArray();
        var dataFields = reader.Schema.GetDataFields()
            .ToDictionary(field => field.Path.ToString(), StringComparer.Ordinal);

        foreach (var binding in plan.Bindings)
        {
            if (!dataFields.TryGetValue(binding.ColumnPath, out var field))
            {
                continue;
            }

            var column = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
            var data = column.Data;
            var length = Math.Min(rowCount, data.Length);
            for (var index = 0; index < length; index++)
            {
                binding.Assign(rows[index], data.GetValue(index));
            }
        }

        return rows;
    }
}
