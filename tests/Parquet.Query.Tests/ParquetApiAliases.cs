#if PARQUET_V6
using System.Collections;
using System.Text;

namespace Parquet.Query.Tests;

internal sealed class CompatibilityParquetOptions : Parquet.ParquetOptions
{
    private string? _footerKey;
    private byte[]? _footerKeyMetadata;
    private string? _signingKey;
    private byte[]? _signingKeyMetadata;
    private string? _aadPrefix;
    private bool _supplyAadPrefix;
    private bool _plaintextFooter;
    private bool _useCtrVariant;

    public CompatibilityParquetOptions()
    {
        ColumnKeys = new ColumnKeyCollection(this);
    }

    public CompatibilityParquetOptions? ParquetOptions
    {
        get => this;
        set
        {
            if (value is null || ReferenceEquals(value, this)) return;
            TreatByteArrayAsString = value.TreatByteArrayAsString;
            TreatBigIntegersAsDates = value.TreatBigIntegersAsDates;
            UseDateOnlyTypeForDates = value.UseDateOnlyTypeForDates;
            DictionaryEncodingThreshold = value.DictionaryEncodingThreshold;
            DictionaryEncodingSampleSize = value.DictionaryEncodingSampleSize;
            DataPageRowCountLimit = value.DataPageRowCountLimit;
            MaximumSmallPoolFreeBytes = value.MaximumSmallPoolFreeBytes;
            MaximumLargePoolFreeBytes = value.MaximumLargePoolFreeBytes;
            UseBigDecimal = value.UseBigDecimal;
            Encryption = value.Encryption;
            Decryption = value.Decryption;
            foreach (var entry in value.ColumnEncodingHints)
            {
                ColumnEncodingHints[entry.Key] = entry.Value;
            }
            foreach (var entry in value.BloomFilterOptionsByColumn)
            {
                BloomFilterOptionsByColumn[entry.Key] = entry.Value;
            }
        }
    }

    public bool UseDictionaryEncoding { get; set; } = true;
    public bool UseDeltaBinaryPackedEncoding { get; set; }
    public bool UseTimeOnlyTypeForTimeMillis { get; set; }
    public bool UseTimeOnlyTypeForTimeMicros { get; set; }

    public string? FooterEncryptionKey
    {
        get => _footerKey;
        set { _footerKey = value; RebuildCryptoOptions(); }
    }

    public byte[]? FooterEncryptionKeyMetadata
    {
        get => _footerKeyMetadata;
        set { _footerKeyMetadata = value; RebuildCryptoOptions(); }
    }

    public string? FooterSigningKey
    {
        get => _signingKey;
        set { _signingKey = value; RebuildCryptoOptions(); }
    }

    public byte[]? FooterSigningKeyMetadata
    {
        get => _signingKeyMetadata;
        set { _signingKeyMetadata = value; RebuildCryptoOptions(); }
    }

    public bool UsePlaintextFooter
    {
        get => _plaintextFooter;
        set { _plaintextFooter = value; RebuildCryptoOptions(); }
    }

    public string? AADPrefix
    {
        get => _aadPrefix;
        set { _aadPrefix = value; RebuildCryptoOptions(); }
    }

    public bool SupplyAadPrefix
    {
        get => _supplyAadPrefix;
        set { _supplyAadPrefix = value; RebuildCryptoOptions(); }
    }

    public bool UseCtrVariant
    {
        get => _useCtrVariant;
        set { _useCtrVariant = value; RebuildCryptoOptions(); }
    }

    public ColumnKeyCollection ColumnKeys { get; }

    public Func<IReadOnlyList<string>, byte[]?, string?>? ColumnKeyResolver { get; set; }

    private void RebuildCryptoOptions()
    {
        var keyText = _footerKey ?? _signingKey;
        if (string.IsNullOrEmpty(keyText)) return;

        var keyBytes = Encoding.UTF8.GetBytes(keyText);
        var metadata = _footerKey is not null ? _footerKeyMetadata : _signingKeyMetadata;
        var encryption = new Parquet.ParquetEncryptionOptions(new Parquet.ParquetKey(keyBytes, metadata ?? Array.Empty<byte>()))
        {
            EncryptFooter = !_plaintextFooter,
            Algorithm = _useCtrVariant ? Parquet.ParquetEncryptionAlgorithm.AesGcmCtrV1 : Parquet.ParquetEncryptionAlgorithm.AesGcmV1,
            AadPrefix = _aadPrefix is null ? null : Encoding.UTF8.GetBytes(_aadPrefix),
            StoreAadPrefix = !_supplyAadPrefix
        };
        foreach (var entry in ColumnKeys)
        {
            encryption.ColumnKeys[entry.Key] = new Parquet.ParquetKey(
                Encoding.UTF8.GetBytes(entry.Value.Key),
                entry.Value.KeyMetadata ?? Array.Empty<byte>());
        }

        Encryption = encryption;
        Decryption = new Parquet.ParquetDecryptionOptions
        {
            FooterKey = keyBytes,
            AadPrefix = encryption.AadPrefix
        };
    }

    public sealed record ColumnKeySpec(string Key, byte[]? KeyMetadata);

    public sealed class ColumnKeyCollection : IEnumerable<KeyValuePair<string, ColumnKeySpec>>
    {
        private readonly CompatibilityParquetOptions _owner;
        private readonly Dictionary<string, ColumnKeySpec> _values = new(StringComparer.Ordinal);

        internal ColumnKeyCollection(CompatibilityParquetOptions owner) => _owner = owner;

        public ColumnKeySpec this[string key]
        {
            get => _values[key];
            set { _values[key] = value; _owner.RebuildCryptoOptions(); }
        }

        public IEnumerator<KeyValuePair<string, ColumnKeySpec>> GetEnumerator() => _values.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
#endif
