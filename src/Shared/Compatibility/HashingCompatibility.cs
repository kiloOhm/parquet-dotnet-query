using System.Security.Cryptography;
using System.Text;

namespace Parquet.Query.Compatibility;

internal static class HashingCompatibility
{
    public static string Sha256Hex(string value)
    {
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(value));
        return HexEncoding.ToHexString(hash);
    }
}
