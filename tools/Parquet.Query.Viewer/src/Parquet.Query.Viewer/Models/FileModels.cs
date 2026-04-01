namespace Parquet.Query.Viewer.Models;

public sealed record ParquetFileInfo(
    string Path,
    long FileSize,
    int RowGroupCount,
    long TotalRowCount,
    SchemaInfo Schema,
    bool IsEncrypted,
    string FileFormat);

public sealed record SchemaInfo(ColumnInfo[] Columns);

public sealed record ColumnInfo(
    string Name,
    string Path,
    string DataType,
    string ClrType,
    bool IsNullable,
    bool IsRepeated);

public sealed record RowGroupInfo(
    int Index,
    long RowCount,
    ColumnChunkInfo[] Columns);

public sealed record ColumnChunkInfo(
    string ColumnName,
    string DataType,
    object? MinValue,
    object? MaxValue,
    long? NullCount,
    long? DistinctCount,
    string? Compression,
    long CompressedSize,
    long UncompressedSize,
    bool HasColumnIndex,
    bool HasOffsetIndex);

public sealed record FileMetadataInfo(
    ParquetFileInfo File,
    RowGroupInfo[] RowGroups,
    FooterInfo Footer,
    EncryptionInfo? Encryption);

public sealed record FooterInfo(
    string CreatedBy,
    int Version,
    long FooterLength,
    Dictionary<string, string> KeyValueMetadata);

public sealed record EncryptionInfo(
    bool IsEncrypted,
    bool HasEncryptedFooter,
    string Algorithm,
    string[] EncryptedColumns);

public sealed record DataPage(
    string[] Columns,
    string[] DataTypes,
    object?[][] Rows,
    int Offset,
    int Limit,
    long TotalRows);

public sealed record ColumnIndexInfo(
    string ColumnPath,
    string IndexType,
    string Description,
    string[] AcceleratedOperations,
    IndexStats? Stats = null);

public sealed record IndexStats(
    long PayloadBytes,
    int? TermCount = null,
    int? DistinctValueCount = null,
    int EntryCount = 0);

public sealed record IndexEntriesPage(
    IndexEntry[] Entries,
    int TotalEntries,
    int Offset,
    int Limit);

/// <summary>
/// A single browsable entry in a footer index: a key (term, value, or bucket id)
/// mapped to the row group indices where it appears.
/// </summary>
public sealed record IndexEntry(
    string Key,
    int[] RowGroups);

public sealed record BuiltinColumnInfo(
    string ColumnPath,
    bool HasStatistics,
    bool HasBloomFilter,
    bool HasPageIndex,
    string? SortOrder);

public sealed record IndicesInfo(
    ColumnIndexInfo[] CustomIndices,
    BuiltinColumnInfo[] BuiltinInfo,
    string[] SortingColumns);

public sealed record EncryptionConfig(
    string? FooterKey = null,
    string? FooterSigningKey = null,
    bool PlaintextFooter = false,
    string? AadPrefix = null,
    bool UseCtr = false,
    Dictionary<string, string>? ColumnKeys = null);
