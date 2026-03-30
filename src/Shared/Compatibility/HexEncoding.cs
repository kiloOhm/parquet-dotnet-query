using System.Text;

namespace Parquet.Query.Compatibility;

internal static class HexEncoding
{
    public static string ToHexString(byte[] bytes)
    {
        var builder = new StringBuilder(bytes.Length * 2);

        for (var index = 0; index < bytes.Length; index++)
        {
            builder.Append(bytes[index].ToString("X2"));
        }

        return builder.ToString();
    }
}
