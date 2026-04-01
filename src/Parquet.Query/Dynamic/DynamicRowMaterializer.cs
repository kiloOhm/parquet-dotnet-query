using Parquet;
using Parquet.Query.Planning;
using Parquet.Schema;

namespace Parquet.Query.Dynamic;

internal static class DynamicRowMaterializer
{
    public static async Task<IReadOnlyList<DynamicRow>> ReadRowGroupAsync(
        ParquetReader reader,
        int rowGroupIndex,
        IReadOnlyList<DataField> columns,
        IReadOnlyList<RowInterval> candidateIntervals,
        CancellationToken cancellationToken)
    {
        using var rgReader = reader.OpenRowGroupReader(rowGroupIndex);
        var rowCount = rgReader.RowCount;
        var rowIndexes = ExpandRowIndexes(rowCount, candidateIntervals);
        var rows = new DynamicRow[rowIndexes.Length];
        for (var i = 0; i < rows.Length; i++)
            rows[i] = new DynamicRow();

        foreach (var field in columns)
        {
            if (!rgReader.ColumnExists(field))
                continue;

            var dataColumn = await rgReader.ReadColumnAsync(field, cancellationToken).ConfigureAwait(false);
            var data = dataColumn.Data;
            var columnPath = field.Path.ToString();
            var isFullCoverage = rowIndexes.Length == rowCount && IsIdentityMap(rowIndexes);

            if (isFullCoverage)
            {
                for (var i = 0; i < rowIndexes.Length; i++)
                    rows[i].Values[columnPath] = i < data.Length ? data.GetValue(i) : null;
            }
            else
            {
                for (var i = 0; i < rowIndexes.Length; i++)
                {
                    var idx = rowIndexes[i];
                    rows[i].Values[columnPath] = idx < data.Length ? data.GetValue(idx) : null;
                }
            }
        }

        return rows;
    }

    private static int[] ExpandRowIndexes(long rowCount, IReadOnlyList<RowInterval> intervals)
    {
        if (intervals.Count == 1 && intervals[0].Start == 0 && intervals[0].End == rowCount)
        {
            return CreateIdentityMap((int)rowCount);
        }

        var total = 0;
        for (var i = 0; i < intervals.Count; i++)
            total += (int)(intervals[i].End - intervals[i].Start);

        if (total == (int)rowCount)
        {
            return CreateIdentityMap((int)rowCount);
        }

        var indexes = new int[total];
        var pos = 0;
        for (var i = 0; i < intervals.Count; i++)
        {
            var interval = intervals[i];
            for (var r = interval.Start; r < interval.End; r++)
                indexes[pos++] = (int)r;
        }

        return indexes;
    }

    private static int[] CreateIdentityMap(int count)
    {
        var map = new int[count];
        for (var i = 0; i < count; i++)
            map[i] = i;
        return map;
    }

    private static bool IsIdentityMap(int[] indexes)
    {
        for (var i = 0; i < indexes.Length; i++)
        {
            if (indexes[i] != i)
                return false;
        }

        return true;
    }
}
