using Parquet.Serialization;

namespace Parquet.Query.Internal;

internal static class ParquetSerializerCompatibility
{
    public static async Task<IReadOnlyList<T>> DeserializeRowGroupAsync<T>(
        string filePath,
        ParquetReader reader,
        int rowGroupIndex,
        QueryParquetSerializerOptions? options,
        CancellationToken cancellationToken)
        where T : class, new()
    {
#if PARQUET_V6
        var result = await ParquetSerializer.DeserializeAsync<T>(
            filePath,
            options,
            rowGroupIndex,
            cancellationToken).ConfigureAwait(false);
        return result.Data.ToArray();
#else
        var rows = new List<T>();
        await ParquetSerializer.DeserializeAsync(
            reader,
            rowGroupIndex,
            rows,
            cancellationToken,
            resultsAlreadyAllocated: false,
            options: options).ConfigureAwait(false);
        return rows.ToArray();
#endif
    }
}
