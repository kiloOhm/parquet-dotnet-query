export interface ParquetFileInfo {
  path: string
  fileSize: number
  rowGroupCount: number
  totalRowCount: number
  schema: SchemaInfo
  isEncrypted: boolean
  fileFormat: string
}

export interface SchemaInfo {
  columns: ColumnInfo[]
}

export interface ColumnInfo {
  name: string
  path: string
  dataType: string
  clrType: string
  isNullable: boolean
  isRepeated: boolean
}

export interface RowGroupInfo {
  index: number
  rowCount: number
  columns: ColumnChunkInfo[]
}

export interface ColumnChunkInfo {
  columnName: string
  dataType: string
  minValue: unknown
  maxValue: unknown
  nullCount: number | null
  distinctCount: number | null
  compression: string | null
  compressedSize: number
  uncompressedSize: number
  hasColumnIndex: boolean
  hasOffsetIndex: boolean
}

export interface FileMetadataInfo {
  file: ParquetFileInfo
  rowGroups: RowGroupInfo[]
  footer: FooterInfo
  encryption: EncryptionInfo | null
}

export interface FooterInfo {
  createdBy: string
  version: number
  footerLength: number
  keyValueMetadata: Record<string, string>
}

export interface EncryptionInfo {
  isEncrypted: boolean
  hasEncryptedFooter: boolean
  algorithm: string
  encryptedColumns: string[]
}

export interface DataPage {
  columns: string[]
  dataTypes: string[]
  rows: unknown[][]
  offset: number
  limit: number
  totalRows: number
}

export interface IndexEntry {
  key: string
  rowGroups: number[]
}

export interface IndexStats {
  payloadBytes: number
  termCount?: number
  distinctValueCount?: number
  entryCount: number
}

export interface IndexEntriesPage {
  entries: IndexEntry[]
  totalEntries: number
  offset: number
  limit: number
}

export interface ColumnIndexInfo {
  columnPath: string
  indexType: string
  description: string
  acceleratedOperations: string[]
  stats?: IndexStats | null
}

export interface BuiltinColumnInfo {
  columnPath: string
  hasStatistics: boolean
  hasBloomFilter: boolean
  hasPageIndex: boolean
  sortOrder: string | null
}

export interface IndicesInfo {
  customIndices: ColumnIndexInfo[]
  builtinInfo: BuiltinColumnInfo[]
  sortingColumns: string[]
}

export interface EncryptionConfig {
  footerKey?: string
  footerSigningKey?: string
  plaintextFooter?: boolean
  aadPrefix?: string
  useCtr?: boolean
  columnKeys?: Record<string, string>
}

export interface QueryPredicate {
  column: string
  operator: string
  value: string
  value2?: string
  maxEdits?: number
  prefixLength?: number
  transpositions?: boolean
}

export interface QueryRequest {
  predicates: QueryPredicate[]
  offset?: number
  limit?: number
}

export interface QueryPlan {
  totalRowGroups: number
  selectedRowGroups: number
  skippedRowGroups: number
  decisions: RowGroupDecision[]
  totalRows: number
  candidateRows: number
  matchedRows: number
  executionMs: number
}

export interface RowGroupDecision {
  index: number
  shouldRead: boolean
  reason: string
  rowCount: number
}

export interface QueryResult {
  plan: QueryPlan
  data: DataPage
}

export interface PickFileResult {
  cancelled: boolean
  file?: ParquetFileInfo
  path?: string
  needsEncryption?: boolean
  error?: string
}
