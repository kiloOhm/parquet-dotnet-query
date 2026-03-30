namespace Parquet.Query;

/// <summary>
/// Stores query planning artifacts in a bounded in-memory least-recently-used cache.
/// </summary>
public sealed class LruParquetQueryCache : IParquetQueryCache
{
    private readonly int _capacity;
    private readonly object _gate = new();
    private readonly Dictionary<string, LinkedListNode<CacheEntry>> _entries = new(StringComparer.Ordinal);
    private readonly LinkedList<CacheEntry> _lru = new();

    /// <summary>
    /// Initializes a new bounded in-memory query cache.
    /// </summary>
    /// <param name="capacity">The maximum number of cached query entries to keep.</param>
    public LruParquetQueryCache(int capacity = 256)
    {
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity));
        }

        _capacity = capacity;
    }

    /// <inheritdoc />
    public ValueTask<object?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrWhiteSpace(key, nameof(key));

        lock (_gate)
        {
            if (!_entries.TryGetValue(key, out LinkedListNode<CacheEntry>? node))
            {
                return ValueTaskCompatibility.FromResult<object?>(null);
            }

            _lru.Remove(node);
            _lru.AddFirst(node);
            return ValueTaskCompatibility.FromResult<object?>(node.Value.Value);
        }
    }

    /// <inheritdoc />
    public ValueTask SetAsync(string key, object value, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrWhiteSpace(key, nameof(key));
        Guard.NotNull(value, nameof(value));

        lock (_gate)
        {
            if (_entries.TryGetValue(key, out LinkedListNode<CacheEntry>? existingNode))
            {
                existingNode.Value = new CacheEntry(key, value);
                _lru.Remove(existingNode);
                _lru.AddFirst(existingNode);
            }
            else
            {
                var node = new LinkedListNode<CacheEntry>(new CacheEntry(key, value));
                _entries[key] = node;
                _lru.AddFirst(node);
            }

            while (_entries.Count > _capacity && _lru.Last is not null)
            {
                var oldest = _lru.Last;
                _lru.RemoveLast();
                _entries.Remove(oldest.Value.Key);
            }
        }

        return ValueTaskCompatibility.CompletedTask;
    }

    private sealed record CacheEntry(string Key, object Value);
}
