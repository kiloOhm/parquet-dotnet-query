using System.Security.Cryptography;
using System.Text;
using Parquet.Query.Compatibility;

namespace Parquet.Query.Internal;

internal static class ParquetKeyDerivation
{
    public static byte[] FromString(string key)
    {
        Guard.NotNullOrWhiteSpace(key, nameof(key));

        if (TryDecodeHex(key, out var decoded) || TryDecodeBase64(key, out decoded))
        {
            return decoded;
        }

        var utf8Key = Encoding.UTF8.GetBytes(key);
        if (IsValidAesKeyLength(utf8Key.Length))
        {
            return utf8Key;
        }

        using var sha256 = SHA256.Create();
        return sha256.ComputeHash(utf8Key);
    }

    private static bool TryDecodeHex(string key, out byte[] decoded)
    {
        decoded = Array.Empty<byte>();
        if (key.Length is not (32 or 48 or 64))
        {
            return false;
        }

        var bytes = new byte[key.Length / 2];
        for (var index = 0; index < bytes.Length; index++)
        {
            var high = HexValue(key[index * 2]);
            var low = HexValue(key[(index * 2) + 1]);
            if (high < 0 || low < 0)
            {
                return false;
            }

            bytes[index] = (byte)((high << 4) | low);
        }

        decoded = bytes;
        return true;
    }

    private static bool TryDecodeBase64(string key, out byte[] decoded)
    {
        decoded = Array.Empty<byte>();
        var normalized = key.Replace('-', '+').Replace('_', '/');
        switch (normalized.Length % 4)
        {
            case 2:
                normalized += "==";
                break;
            case 3:
                normalized += "=";
                break;
            case 1:
                return false;
        }

        try
        {
            var bytes = Convert.FromBase64String(normalized);
            if (!IsValidAesKeyLength(bytes.Length))
            {
                return false;
            }

            decoded = bytes;
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static int HexValue(char value)
    {
        if (value is >= '0' and <= '9') return value - '0';
        if (value is >= 'A' and <= 'F') return value - 'A' + 10;
        if (value is >= 'a' and <= 'f') return value - 'a' + 10;
        return -1;
    }

    private static bool IsValidAesKeyLength(int length) => length is 16 or 24 or 32;
}
