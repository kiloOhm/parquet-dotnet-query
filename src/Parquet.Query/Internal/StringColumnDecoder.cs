#if PARQUET_V6
using System.Buffers;
using Parquet.Schema;

namespace Parquet.Query.Internal;

/// <summary>
/// Decodes a string column into <see cref="string"/> instances, reusing one instance per distinct
/// value instead of allocating one per row.
/// </summary>
/// <remarks>
/// <para>
/// parquet-dotnet v6 represents string data as <see cref="ReadOnlyMemory{T}"/> of char, and its
/// <c>ReadAsync(DataField, Memory&lt;string?&gt;, ...)</c> convenience overload materializes that by
/// allocating a fresh <see cref="string"/> for every row. For the columns this library filters on -
/// entity ids, group names, enum-like text - that is mostly waste: a row group of 5 000 rows commonly
/// holds a few dozen distinct values, and the dictionary page already stored each of them once.
/// </para>
/// <para>
/// This decoder reads the raw char spans and hands out shared instances, which is what makes the
/// difference measurable: a filter column read stops scaling with row count and starts scaling with
/// distinct value count.
/// </para>
/// <para>
/// The reuse cache is a fixed-size direct-mapped table. A miss costs one hash and one span comparison
/// and then allocates exactly what the previous behaviour would have, so a genuinely unique column is
/// no worse off than before; a repetitive one collapses to a handful of allocations.
/// </para>
/// </remarks>
internal static class StringColumnDecoder
{
    /// <summary>
    /// Slot count of the reuse table. Power of two so the index is a mask, and large enough to hold the
    /// distinct values of a typical low-cardinality column without eviction.
    /// </summary>
    private const int CacheSlots = 1024;

    private const int CacheMask = CacheSlots - 1;

    /// <summary>
    /// Reads a string column for the current row group.
    /// </summary>
    /// <param name="rowGroupReader">The row group to read from.</param>
    /// <param name="field">The string column.</param>
    /// <param name="rowCount">The row count of the row group.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The decoded values, with nulls where the column is null.</returns>
    public static async Task<string?[]> ReadAsync(
        QueryParquetRowGroupReader rowGroupReader,
        DataField field,
        int rowCount,
        CancellationToken cancellationToken)
    {
        var values = new string?[rowCount];
        if (rowCount == 0)
        {
            return values;
        }

        // Raw reads return non-null values packed together, with the nulls described by definition
        // levels, so a nullable column needs both buffers and an expansion pass.
        var hasDefinitionLevels = field.MaxDefinitionLevel > 0;
        var rawValues = ArrayPool<ReadOnlyMemory<char>>.Shared.Rent(rowCount);
        var definitionLevels = hasDefinitionLevels
            ? ArrayPool<int>.Shared.Rent(rowCount)
            : Array.Empty<int>();

        try
        {
            await rowGroupReader.ReadRawAsync(
                field,
                rawValues.AsMemory(0, rowCount),
                hasDefinitionLevels ? definitionLevels.AsMemory(0, rowCount) : null,
                null,
                cancellationToken).ConfigureAwait(false);

            var cache = ArrayPool<string?>.Shared.Rent(StringColumnDecoder.CacheSlots);
            try
            {
                // Rented arrays are not zeroed, so the slots have to be cleared before they are trusted
                // as cache entries; a stale reference from a previous tenant would be handed out as a
                // decoded value.
                Array.Clear(cache, 0, StringColumnDecoder.CacheSlots);

                // A sort-key column arrives as long runs of one value, so the previous result is checked
                // before the table is consulted at all: that turns the common case into a single span
                // comparison with no hashing.
                string? previous = null;

                if (hasDefinitionLevels)
                {
                    var valueIndex = 0;
                    for (var index = 0; index < rowCount; index++)
                    {
                        if (definitionLevels[index] == 0)
                        {
                            values[index] = null;
                            continue;
                        }

                        values[index] = previous = StringColumnDecoder.Resolve(cache, previous, rawValues[valueIndex++].Span);
                    }
                }
                else
                {
                    for (var index = 0; index < rowCount; index++)
                    {
                        values[index] = previous = StringColumnDecoder.Resolve(cache, previous, rawValues[index].Span);
                    }
                }
            }
            finally
            {
                // Returned cleared: the rented array can be longer than the slot count, so clearing only
                // the slots would leave the decoded strings reachable through the pool.
                ArrayPool<string?>.Shared.Return(cache, clearArray: true);
            }
        }
        finally
        {
            ArrayPool<ReadOnlyMemory<char>>.Shared.Return(rawValues, clearArray: true);
            if (hasDefinitionLevels)
            {
                ArrayPool<int>.Shared.Return(definitionLevels);
            }
        }

        return values;
    }

    private static string Resolve(string?[] cache, string? previous, ReadOnlySpan<char> span)
    {
        if (previous is not null && span.SequenceEqual(previous.AsSpan()))
        {
            return previous;
        }

        var slot = StringColumnDecoder.Hash(span) & StringColumnDecoder.CacheMask;
        var cached = cache[slot];
        if (cached is not null && span.SequenceEqual(cached.AsSpan()))
        {
            return cached;
        }

        var created = span.Length == 0 ? string.Empty : new string(span);

        // Direct-mapped: a collision simply replaces the occupant. Columns whose values arrive in runs
        // or from a small dictionary keep hitting; a unique column keeps missing and pays only the hash.
        cache[slot] = created;
        return created;
    }

    private static int Hash(ReadOnlySpan<char> span)
    {
        // FNV-1a over at most the first and last few chars. Entity ids share long prefixes, so hashing
        // the tail matters more than hashing everything, and a bounded probe keeps the cost per value
        // independent of value length.
        const int MaxSampled = 8;
        unchecked
        {
            var hash = (int)2166136261;
            hash = (hash ^ span.Length) * 16777619;

            var sampled = Math.Min(span.Length, MaxSampled);
            for (var index = 0; index < sampled; index++)
            {
                hash = (hash ^ span[span.Length - 1 - index]) * 16777619;
            }

            return hash & 0x7FFFFFFF;
        }
    }
}
#endif
