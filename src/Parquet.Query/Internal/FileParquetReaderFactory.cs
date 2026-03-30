using Parquet;

namespace Parquet.Query.Internal;

internal sealed class FileParquetReaderFactory : IParquetReaderFactory
{
    public static FileParquetReaderFactory Instance { get; } = new();

    private FileParquetReaderFactory()
    {
    }

    public async ValueTask<IParquetReaderLease> RentAsync(
        string filePath,
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        var stream = System.IO.File.OpenRead(filePath);
        try
        {
            var reader = await ParquetReader.CreateAsync(
                stream,
                parquetOptions,
                leaveStreamOpen: false,
                cancellationToken).ConfigureAwait(false);
            return new FileParquetReaderLease(reader, stream);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private sealed class FileParquetReaderLease : IParquetReaderLease
    {
        private readonly Stream _stream;
        private bool _disposed;

        public FileParquetReaderLease(ParquetReader reader, Stream stream)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        public ParquetReader Reader { get; }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Reader.Dispose();
            await _stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
