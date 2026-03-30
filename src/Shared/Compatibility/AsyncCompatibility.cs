using System.IO;

namespace Parquet.Query.Compatibility;

internal static class AsyncCompatibility
{
    public static ValueTask DisposeAsync(IDisposable disposable)
    {
        disposable.Dispose();
        return default;
    }

    public static async Task WaitAsync(Task task, CancellationToken cancellationToken)
    {
#if NET6_0_OR_GREATER
        await task.WaitAsync(cancellationToken).ConfigureAwait(false);
#else
        if (task.IsCompleted || !cancellationToken.CanBeCanceled)
        {
            await task.ConfigureAwait(false);
            return;
        }

        var cancellationTask = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (cancellationToken.Register(static state => ((TaskCompletionSource<object?>)state!).TrySetCanceled(), cancellationTask))
        {
            var completedTask = await Task.WhenAny(task, cancellationTask.Task).ConfigureAwait(false);
            if (!ReferenceEquals(completedTask, task))
            {
                await cancellationTask.Task.ConfigureAwait(false);
            }
        }

        await task.ConfigureAwait(false);
#endif
    }

    public static async Task<byte[]> ReadAllBytesAsync(string filePath, CancellationToken cancellationToken)
    {
#if NET6_0_OR_GREATER
        return await System.IO.File.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false);
#else
        using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, useAsync: true);
        if (stream.Length > int.MaxValue)
        {
            throw new NotSupportedException("Files larger than Int32.MaxValue are not supported.");
        }

        var bytes = new byte[stream.Length];
        var offset = 0;
        while (offset < bytes.Length)
        {
            var read = await stream.ReadAsync(bytes, offset, bytes.Length - offset, cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                throw new EndOfStreamException($"Unexpected end of stream while reading '{filePath}'.");
            }

            offset += read;
        }

        return bytes;
#endif
    }

    public static async Task WriteAllBytesAsync(string filePath, byte[] bytes, CancellationToken cancellationToken)
    {
#if NET6_0_OR_GREATER
        await System.IO.File.WriteAllBytesAsync(filePath, bytes, cancellationToken).ConfigureAwait(false);
#else
        using var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true);
        await stream.WriteAsync(bytes, 0, bytes.Length, cancellationToken).ConfigureAwait(false);
#endif
    }

    public static Task WriteAsync(Stream stream, byte[] bytes, CancellationToken cancellationToken)
    {
#if NET6_0_OR_GREATER
        return stream.WriteAsync(bytes, cancellationToken).AsTask();
#else
        return stream.WriteAsync(bytes, 0, bytes.Length, cancellationToken);
#endif
    }

    public static Task WriteAsync(Stream stream, byte[] bytes, int offset, int count, CancellationToken cancellationToken)
    {
#if NET6_0_OR_GREATER
        return stream.WriteAsync(bytes.AsMemory(offset, count), cancellationToken).AsTask();
#else
        return stream.WriteAsync(bytes, offset, count, cancellationToken);
#endif
    }
}
