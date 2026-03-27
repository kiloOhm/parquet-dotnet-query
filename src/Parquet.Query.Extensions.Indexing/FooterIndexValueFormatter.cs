using System.Globalization;
using System.Text;

namespace Parquet.Query.Extensions.Indexing;

internal static class FooterIndexValueFormatter
{
    public static bool IsSupportedType(Type type)
    {
        var unwrapped = Nullable.GetUnderlyingType(type) ?? type;
        if (unwrapped.IsEnum)
        {
            return true;
        }

        return unwrapped == typeof(string) ||
            unwrapped == typeof(bool) ||
            unwrapped == typeof(byte) ||
            unwrapped == typeof(sbyte) ||
            unwrapped == typeof(short) ||
            unwrapped == typeof(ushort) ||
            unwrapped == typeof(int) ||
            unwrapped == typeof(uint) ||
            unwrapped == typeof(long) ||
            unwrapped == typeof(ulong) ||
            unwrapped == typeof(float) ||
            unwrapped == typeof(double) ||
            unwrapped == typeof(decimal) ||
            unwrapped == typeof(Guid) ||
            unwrapped == typeof(DateTime) ||
            unwrapped == typeof(DateTimeOffset) ||
            unwrapped == typeof(TimeSpan) ||
            unwrapped == typeof(byte[]);
    }

    public static bool TryFormat(object? value, out string formatted)
    {
        switch (value)
        {
            case null:
                formatted = string.Empty;
                return false;
            case string text:
                formatted = text;
                return true;
            case Guid guid:
                formatted = guid.ToString("D");
                return true;
            case DateTime dateTime:
                formatted = dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                return true;
            case DateTimeOffset dateTimeOffset:
                formatted = dateTimeOffset.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                return true;
            case TimeSpan timeSpan:
                formatted = timeSpan.ToString("c", CultureInfo.InvariantCulture);
                return true;
            case byte[] bytes:
                formatted = Convert.ToBase64String(bytes);
                return true;
        }

        var runtimeType = value.GetType();
        if (runtimeType.IsEnum)
        {
            formatted = value.ToString() ?? string.Empty;
            return true;
        }

        if (value is IFormattable formattable)
        {
            formatted = formattable.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty;
            return true;
        }

        formatted = string.Empty;
        return false;
    }

    public static int GetBucket(string value, int bucketCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(bucketCount);

        const ulong offsetBasis = 14695981039346656037;
        const ulong prime = 1099511628211;

        var byteCount = Encoding.UTF8.GetByteCount(value);
        Span<byte> buffer = byteCount <= 256
            ? stackalloc byte[byteCount]
            : new byte[byteCount];
        Encoding.UTF8.GetBytes(value, buffer);

        ulong hash = offsetBasis;
        foreach (var current in buffer)
        {
            hash ^= current;
            hash *= prime;
        }

        return (int)(hash % (uint)bucketCount);
    }
}
