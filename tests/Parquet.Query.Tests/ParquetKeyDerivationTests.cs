using System.Security.Cryptography;
using System.Text;
using Parquet.Query.Internal;

namespace Parquet.Query.Tests;

public sealed class ParquetKeyDerivationTests
{
    [Fact]
    public void FromString_preserves_valid_utf8_key_material()
    {
        const string key = "0123456789ABCDEF";

        Assert.Equal(Encoding.UTF8.GetBytes(key), ParquetKeyDerivation.FromString(key));
    }

    [Fact]
    public void FromString_decodes_hex_key_material()
    {
        const string key = "00112233445566778899AABBCCDDEEFF";

        Assert.Equal(
            new byte[] { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF },
            ParquetKeyDerivation.FromString(key));
    }

    [Fact]
    public void FromString_decodes_base64_key_material()
    {
        var expected = Enumerable.Range(0, 32).Select(value => (byte)value).ToArray();

        Assert.Equal(expected, ParquetKeyDerivation.FromString(Convert.ToBase64String(expected)));
    }

    [Fact]
    public void FromString_decodes_unpadded_base64url_key_material()
    {
        var expected = Enumerable.Range(240, 16).Select(value => (byte)value).ToArray();
        var key = Convert.ToBase64String(expected).TrimEnd('=').Replace('+', '-').Replace('/', '_');

        Assert.Equal(expected, ParquetKeyDerivation.FromString(key));
    }

    [Fact]
    public void FromString_derives_arbitrary_text_with_sha256()
    {
        const string key = "correct horse battery staple";
        byte[] expected;
        using (var sha256 = SHA256.Create())
        {
            expected = sha256.ComputeHash(Encoding.UTF8.GetBytes(key));
        }

        Assert.Equal(expected, ParquetKeyDerivation.FromString(key));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void FromString_rejects_missing_key(string key)
    {
        Assert.Throws<ArgumentException>(() => ParquetKeyDerivation.FromString(key));
    }
}
