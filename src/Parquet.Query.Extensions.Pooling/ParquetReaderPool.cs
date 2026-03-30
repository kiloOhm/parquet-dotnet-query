using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using Parquet;
using Parquet.Query;

namespace Parquet.Query.Extensions.Pooling;

/// <summary>
/// Reuses <see cref="ParquetReader"/> instances per file and coordinates file replacement.
/// </summary>
public sealed class ParquetReaderPool : IParquetReaderFactory, IAsyncDisposable
{
    private static readonly StringComparer FilePathComparer = PlatformCompatibility.IsWindows()
        ? StringComparer.OrdinalIgnoreCase
        : StringComparer.Ordinal;

    private readonly ConcurrentDictionary<string, FilePoolState> _files;
    private readonly int _maxReadersPerFile;
    private bool _disposed;

    /// <summary>
    /// Initializes a new reader pool.
    /// </summary>
    public ParquetReaderPool(ParquetReaderPoolOptions? options = null)
    {
        var poolOptions = options ?? new ParquetReaderPoolOptions();
        if (poolOptions.MaxReadersPerFile <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxReadersPerFile must be greater than zero.");
        }

        _maxReadersPerFile = poolOptions.MaxReadersPerFile;
        _files = new ConcurrentDictionary<string, FilePoolState>(FilePathComparer);
    }

    /// <inheritdoc />
    public async ValueTask<IParquetReaderLease> RentAsync(
        string filePath,
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var normalizedFilePath = NormalizeFilePath(filePath);
        var state = GetOrCreateState(normalizedFilePath);
        var optionsKey = ParquetOptionsKey.Create(parquetOptions);

        while (true)
        {
            ThrowIfDisposed();

            var unblockedTask = GetUnblockedTask(state);
            if (unblockedTask is not null)
            {
                await AsyncCompatibility.WaitAsync(unblockedTask, cancellationToken).ConfigureAwait(false);
                continue;
            }

            await state.Availability.WaitAsync(cancellationToken).ConfigureAwait(false);

            PooledReader? pooledReader = null;
            var createNewReader = false;
            lock (state.Gate)
            {
                if (_disposed)
                {
                    state.Availability.Release();
                    ThrowDisposed();
                }

                if (state.IsBlocked)
                {
                    state.Availability.Release();
                    continue;
                }

                if (!state.Buckets.TryGetValue(optionsKey, out var bucket))
                {
                    bucket = new ReaderBucket();
                    state.Buckets[optionsKey] = bucket;
                }

                if (bucket.IdleReaders.Count > 0)
                {
                    pooledReader = bucket.IdleReaders.Dequeue();
                }
                else
                {
                    bucket.CreatedCount++;
                    createNewReader = true;
                }

                state.ActiveCount++;
            }

            if (createNewReader)
            {
                try
                {
                    pooledReader = await CreateReaderAsync(normalizedFilePath, parquetOptions, optionsKey, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    lock (state.Gate)
                    {
                        state.ActiveCount--;
                        var bucket = state.Buckets[optionsKey];
                        bucket.CreatedCount--;
                        RemoveEmptyBucket(state, optionsKey, bucket);
                        if (state.ActiveCount == 0)
                        {
                            state.DrainCompletion.TrySetResult(null);
                        }
                    }

                    state.Availability.Release();
                    throw;
                }
            }

            return new PooledReaderLease(this, state, pooledReader!);
        }
    }

    /// <summary>
    /// Opens and returns readers for one file so later queries can reuse them immediately.
    /// </summary>
    public async Task PrewarmAsync(
        string filePath,
        int readerCount,
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(filePath, nameof(filePath));
        if (readerCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(readerCount));
        }

        if (readerCount == 0)
        {
            return;
        }

        var warmCount = Math.Min(readerCount, _maxReadersPerFile);
        var leases = new List<IParquetReaderLease>(warmCount);
        try
        {
            for (var index = 0; index < warmCount; index++)
            {
                leases.Add(await RentAsync(filePath, parquetOptions, cancellationToken).ConfigureAwait(false));
            }
        }
        finally
        {
            for (var index = leases.Count - 1; index >= 0; index--)
            {
                await leases[index].DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Prewarms multiple files with the same reader count and parquet options.
    /// </summary>
    public Task PrewarmAsync(
        IEnumerable<string> filePaths,
        int readerCount,
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(filePaths, nameof(filePaths));

        var tasks = filePaths
            .Where(path => !string.IsNullOrWhiteSpace(path))
            .Select(path => PrewarmAsync(path, readerCount, parquetOptions, cancellationToken));
        return Task.WhenAll(tasks);
    }

    /// <summary>
    /// Blocks a file from new rentals, drains existing readers, and returns a lease that keeps it blocked.
    /// </summary>
    public async Task<ParquetReaderPoolFileBlockLease> BlockFileAsync(
        string filePath,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var normalizedFilePath = NormalizeFilePath(filePath);
        var state = GetOrCreateState(normalizedFilePath);

        await state.BlockSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        List<PooledReader>? idleReaders = null;
        var blockSemaphoreHeld = true;
        try
        {
            Task drainTask;
            lock (state.Gate)
            {
                if (_disposed)
                {
                    state.BlockSemaphore.Release();
                    blockSemaphoreHeld = false;
                    ThrowDisposed();
                }

                state.IsBlocked = true;
                state.UnblockedCompletion = CreatePendingCompletion();
                state.DrainCompletion = state.ActiveCount == 0
                    ? CreateCompletedCompletion()
                    : CreatePendingCompletion();
                idleReaders = DrainIdleReaders(state);
                drainTask = state.DrainCompletion.Task;
            }

            await DisposeReadersAsync(idleReaders).ConfigureAwait(false);
            await AsyncCompatibility.WaitAsync(drainTask, cancellationToken).ConfigureAwait(false);
            return new ParquetReaderPoolFileBlockLease(this, normalizedFilePath, state);
        }
        catch
        {
            lock (state.Gate)
            {
                state.IsBlocked = false;
                state.DrainCompletion.TrySetResult(null);
                state.UnblockedCompletion.TrySetResult(null);
            }

            if (blockSemaphoreHeld)
            {
                state.BlockSemaphore.Release();
            }

            throw;
        }
    }

    /// <summary>
    /// Drains idle readers and waits for outstanding leases to be returned before disposing the pool.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        var disposeTasks = new List<Task>();
        foreach (var state in _files.Values)
        {
            List<PooledReader> idleReaders;
            Task drainTask;
            lock (state.Gate)
            {
                state.UnblockedCompletion.TrySetResult(null);
                idleReaders = DrainIdleReaders(state);
                if (state.ActiveCount == 0)
                {
                    state.DrainCompletion.TrySetResult(null);
                }
                else if (state.DrainCompletion.Task.IsCompleted)
                {
                    state.DrainCompletion = CreatePendingCompletion();
                }

                drainTask = state.DrainCompletion.Task;
            }

            disposeTasks.Add(DisposeReadersAndAwaitDrainAsync(idleReaders, drainTask));
        }

        await Task.WhenAll(disposeTasks).ConfigureAwait(false);
    }

    internal async ValueTask ReturnAsync(FilePoolState state, PooledReader reader)
    {
        var shouldDispose = false;

        lock (state.Gate)
        {
            state.ActiveCount--;

            if (_disposed || state.IsBlocked)
            {
                shouldDispose = true;
                if (state.Buckets.TryGetValue(reader.OptionsKey, out var bucket))
                {
                    bucket.CreatedCount--;
                    RemoveEmptyBucket(state, reader.OptionsKey, bucket);
                }
            }
            else
            {
                state.Buckets[reader.OptionsKey].IdleReaders.Enqueue(reader);
            }

            if (state.ActiveCount == 0)
            {
                state.DrainCompletion.TrySetResult(null);
            }
        }

        if (shouldDispose)
        {
            await reader.DisposeAsync().ConfigureAwait(false);
        }

        state.Availability.Release();
        TryRemoveState(state);
    }

    internal ValueTask ReleaseFileBlockAsync(FilePoolState state)
    {
        lock (state.Gate)
        {
            state.IsBlocked = false;
            state.DrainCompletion.TrySetResult(null);
            state.UnblockedCompletion.TrySetResult(null);
        }

        state.BlockSemaphore.Release();
        TryRemoveState(state);
        return ValueTaskCompatibility.CompletedTask;
    }

    internal sealed class FilePoolState
    {
        public FilePoolState(string filePath, int maxReadersPerFile)
        {
            FilePath = filePath;
            Availability = new SemaphoreSlim(maxReadersPerFile, maxReadersPerFile);
        }

        public string FilePath { get; }

        public object Gate { get; } = new();

        public SemaphoreSlim Availability { get; }

        public SemaphoreSlim BlockSemaphore { get; } = new(1, 1);

        public Dictionary<ParquetOptionsKey, ReaderBucket> Buckets { get; } = new();

        public TaskCompletionSource<object?> DrainCompletion { get; set; } = CreateCompletedCompletion();

        public TaskCompletionSource<object?> UnblockedCompletion { get; set; } = CreateCompletedCompletion();

        public int ActiveCount { get; set; }

        public bool IsBlocked { get; set; }
    }

    internal sealed class ReaderBucket
    {
        public Queue<PooledReader> IdleReaders { get; } = new();

        public int CreatedCount { get; set; }
    }

    internal sealed class PooledReader
    {
        private readonly Stream _stream;
        private bool _disposed;

        public PooledReader(ParquetReader reader, Stream stream, ParquetOptionsKey optionsKey)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            OptionsKey = optionsKey ?? throw new ArgumentNullException(nameof(optionsKey));
        }

        public ParquetReader Reader { get; }

        public ParquetOptionsKey OptionsKey { get; }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Reader.Dispose();
            await AsyncCompatibility.DisposeAsync(_stream).ConfigureAwait(false);
        }
    }

    private sealed class PooledReaderLease : IParquetReaderLease
    {
        private readonly ParquetReaderPool _pool;
        private readonly FilePoolState _state;
        private readonly PooledReader _reader;
        private int _disposed;

        public PooledReaderLease(ParquetReaderPool pool, FilePoolState state, PooledReader reader)
        {
            _pool = pool ?? throw new ArgumentNullException(nameof(pool));
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        }

        public ParquetReader Reader => _reader.Reader;

        public ValueTask DisposeAsync() =>
            Interlocked.Exchange(ref _disposed, 1) == 0
                ? _pool.ReturnAsync(_state, _reader)
                : ValueTaskCompatibility.CompletedTask;
    }

    private FilePoolState GetOrCreateState(string filePath) =>
        _files.GetOrAdd(filePath, path => new FilePoolState(path, _maxReadersPerFile));

    private static string NormalizeFilePath(string filePath)
    {
        Guard.NotNullOrWhiteSpace(filePath, nameof(filePath));
        return Path.GetFullPath(filePath);
    }

    private static Task? GetUnblockedTask(FilePoolState state)
    {
        lock (state.Gate)
        {
            return state.IsBlocked
                ? state.UnblockedCompletion.Task
                : null;
        }
    }

    private static List<PooledReader> DrainIdleReaders(FilePoolState state)
    {
        var drainedReaders = new List<PooledReader>();
        var emptyKeys = new List<ParquetOptionsKey>();

        foreach (var entry in state.Buckets)
        {
            while (entry.Value.IdleReaders.Count > 0)
            {
                drainedReaders.Add(entry.Value.IdleReaders.Dequeue());
                entry.Value.CreatedCount--;
            }

            if (entry.Value.CreatedCount == 0)
            {
                emptyKeys.Add(entry.Key);
            }
        }

        foreach (var emptyKey in emptyKeys)
        {
            state.Buckets.Remove(emptyKey);
        }

        return drainedReaders;
    }

    private static void RemoveEmptyBucket(FilePoolState state, ParquetOptionsKey optionsKey, ReaderBucket bucket)
    {
        if (bucket.CreatedCount == 0 && bucket.IdleReaders.Count == 0)
        {
            state.Buckets.Remove(optionsKey);
        }
    }

    private async Task<PooledReader> CreateReaderAsync(
        string filePath,
        ParquetOptions? parquetOptions,
        ParquetOptionsKey optionsKey,
        CancellationToken cancellationToken)
    {
        var stream = System.IO.File.OpenRead(filePath);
        try
        {
            var reader = await ParquetReader.CreateAsync(
                stream,
                parquetOptions,
                leaveStreamOpen: false,
                cancellationToken).ConfigureAwait(false);
            return new PooledReader(reader, stream, optionsKey);
        }
        catch
        {
            await AsyncCompatibility.DisposeAsync(stream).ConfigureAwait(false);
            throw;
        }
    }

    private static async Task DisposeReadersAsync(IEnumerable<PooledReader> readers)
    {
        foreach (var reader in readers)
        {
            await reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task DisposeReadersAndAwaitDrainAsync(
        IReadOnlyList<PooledReader> idleReaders,
        Task drainTask)
    {
        await DisposeReadersAsync(idleReaders).ConfigureAwait(false);
        await drainTask.ConfigureAwait(false);
    }

    private void TryRemoveState(FilePoolState state)
    {
        if (_disposed)
        {
            _files.TryRemove(state.FilePath, out _);
            return;
        }

        lock (state.Gate)
        {
            if (state.ActiveCount == 0 &&
                !state.IsBlocked &&
                state.Buckets.Count == 0 &&
                state.BlockSemaphore.CurrentCount > 0)
            {
                _files.TryRemove(state.FilePath, out _);
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            ThrowDisposed();
        }
    }

    private void ThrowDisposed() => throw new ObjectDisposedException(nameof(ParquetReaderPool));

    private static TaskCompletionSource<object?> CreateCompletedCompletion()
    {
        var completion = CreatePendingCompletion();
        completion.TrySetResult(null);
        return completion;
    }

    private static TaskCompletionSource<object?> CreatePendingCompletion() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal sealed class ParquetOptionsKey : IEquatable<ParquetOptionsKey>
    {
        private readonly string _fingerprint;

        private ParquetOptionsKey(string fingerprint)
        {
            _fingerprint = fingerprint;
        }

        public static ParquetOptionsKey Create(ParquetOptions? options)
        {
            if (options is null)
            {
                return new ParquetOptionsKey("default");
            }

            var builder = new StringBuilder();
            Append(builder, nameof(options.TreatByteArrayAsString), options.TreatByteArrayAsString);
            Append(builder, nameof(options.TreatBigIntegersAsDates), options.TreatBigIntegersAsDates);
            Append(builder, nameof(options.UseDateOnlyTypeForDates), options.UseDateOnlyTypeForDates);
            Append(builder, nameof(options.UseTimeOnlyTypeForTimeMillis), options.UseTimeOnlyTypeForTimeMillis);
            Append(builder, nameof(options.UseTimeOnlyTypeForTimeMicros), options.UseTimeOnlyTypeForTimeMicros);
            Append(builder, nameof(options.UseDictionaryEncoding), options.UseDictionaryEncoding);
            Append(builder, nameof(options.DictionaryEncodingThreshold), options.DictionaryEncodingThreshold);
            Append(builder, nameof(options.UseDeltaBinaryPackedEncoding), options.UseDeltaBinaryPackedEncoding);
            Append(builder, nameof(options.MaximumSmallPoolFreeBytes), options.MaximumSmallPoolFreeBytes);
            Append(builder, nameof(options.MaximumLargePoolFreeBytes), options.MaximumLargePoolFreeBytes);
            Append(builder, nameof(options.UseBigDecimal), options.UseBigDecimal);
            Append(builder, nameof(options.UsePlaintextFooter), options.UsePlaintextFooter);
            Append(builder, nameof(options.FooterEncryptionKey), options.FooterEncryptionKey);
            Append(builder, nameof(options.FooterEncryptionKeyMetadata), HexEncoding.ToHexString(options.FooterEncryptionKeyMetadata ?? Array.Empty<byte>()));
            Append(builder, nameof(options.FooterSigningKey), options.FooterSigningKey);
            Append(builder, nameof(options.FooterSigningKeyMetadata), HexEncoding.ToHexString(options.FooterSigningKeyMetadata ?? Array.Empty<byte>()));
            Append(builder, nameof(options.AADPrefix), options.AADPrefix);
            Append(builder, nameof(options.SupplyAadPrefix), options.SupplyAadPrefix);
            Append(builder, nameof(options.UseCtrVariant), options.UseCtrVariant);

            foreach (var entry in options.BloomFilterOptionsByColumn.OrderBy(entry => entry.Key, StringComparer.Ordinal))
            {
                Append(builder, $"bloom:{entry.Key}", $"{entry.Value.EnableBloomFilters}|{entry.Value.BloomFilterFpp}|{entry.Value.BloomFilterBitsPerValueOverride}");
            }

            foreach (var entry in options.ColumnKeys.OrderBy(entry => entry.Key, StringComparer.Ordinal))
            {
                Append(builder, $"column:{entry.Key}", $"{entry.Value.Key}|{HexEncoding.ToHexString(entry.Value.KeyMetadata ?? Array.Empty<byte>())}");
            }

            if (options.ColumnKeyResolver is not null)
            {
                var method = options.ColumnKeyResolver.Method;
                Append(
                    builder,
                    nameof(options.ColumnKeyResolver),
                    $"{method.Module.ModuleVersionId}:{method.MetadataToken}:{RuntimeHelpers.GetHashCode(options.ColumnKeyResolver.Target ?? options.ColumnKeyResolver)}");
            }

            return new ParquetOptionsKey(builder.ToString());
        }

        public bool Equals(ParquetOptionsKey? other) =>
            other is not null &&
            string.Equals(_fingerprint, other._fingerprint, StringComparison.Ordinal);

        public override bool Equals(object? obj) => obj is ParquetOptionsKey other && Equals(other);

        public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(_fingerprint);

        private static void Append(StringBuilder builder, string name, object? value)
        {
            builder.Append(name);
            builder.Append('=');
            builder.Append(value);
            builder.Append(';');
        }
    }
}
