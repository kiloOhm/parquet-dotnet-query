using System.Collections.Concurrent;

namespace Parquet.Query.Internal;

internal static class BoundedPlannerCache
{
    public static void Set<TKey, TValue>(
        ConcurrentDictionary<TKey, TValue> cache,
        TKey key,
        TValue value,
        int maxEntries,
        Func<TValue, long> accessTicksSelector)
        where TKey : notnull
    {
        cache[key] = value;

        if (cache.Count <= maxEntries)
        {
            return;
        }

        var overflow = cache.Count - maxEntries;
        foreach (var entry in cache
                     .OrderBy(pair => accessTicksSelector(pair.Value))
                     .Take(overflow)
                     .ToArray())
        {
            cache.TryRemove(entry.Key, out _);
        }
    }
}
