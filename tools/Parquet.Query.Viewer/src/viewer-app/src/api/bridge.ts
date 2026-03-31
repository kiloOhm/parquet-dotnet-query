import type {
  DataPage,
  EncryptionConfig,
  FileMetadataInfo,
  IndicesInfo,
  ParquetFileInfo,
  PickFileResult,
  QueryPlan,
  QueryPredicate,
  QueryResult,
} from './types'

type PendingRequest = {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
}

const pending = new Map<string, PendingRequest>()
let requestId = 0

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const win = window as any

function isWebView(): boolean {
  return typeof window !== 'undefined' && !!win.chrome?.webview
}

function getWebView() {
  return win.chrome.webview as {
    postMessage: (msg: string) => void
    addEventListener: (event: string, handler: (e: { data: string }) => void) => void
  }
}

if (isWebView()) {
  const wv = getWebView()
  wv.addEventListener('message', (e: { data: unknown }) => {
    try {
      // PostWebMessageAsJson delivers e.data as an already-parsed JS object
      const msg = (typeof e.data === 'string' ? JSON.parse(e.data) : e.data) as {
        id: string; result?: unknown; error?: string
      }
      const req = pending.get(msg.id)
      if (!req) return
      pending.delete(msg.id)
      if (msg.error) {
        req.reject(new Error(msg.error))
      } else {
        req.resolve(msg.result)
      }
    } catch (err) {
      console.error('[bridge] Failed to handle message:', err, e.data)
    }
  })
}

function invoke<T>(method: string, params?: unknown): Promise<T> {
  if (!isWebView()) {
    return mockInvoke<T>(method, params)
  }

  const id = String(++requestId)
  return new Promise<T>((resolve, reject) => {
    pending.set(id, {
      resolve: resolve as (value: unknown) => void,
      reject,
    })
    const wv = getWebView()
    wv.postMessage(JSON.stringify({ id, method, params }))
  })
}

// Mock data for browser development
function mockInvoke<T>(method: string, _params?: unknown): Promise<T> {
  const mockColumns = ['id', 'name', 'age', 'city', 'salary', 'department', 'hire_date', 'active']
  const mockTypes = ['Int64', 'String', 'Int32', 'String', 'Double', 'String', 'DateTimeOffset', 'Boolean']
  const cities = ['New York', 'London', 'Tokyo', 'Berlin', 'Paris', 'Sydney']
  const depts = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance']
  const names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Hank']

  const mockRow = (i: number): unknown[] => [
    i + 1,
    names[i % names.length],
    25 + (i % 40),
    cities[i % cities.length],
    50000 + (i * 1234.56) % 100000,
    depts[i % depts.length],
    `2020-0${(i % 9) + 1}-15T00:00:00+00:00`,
    i % 3 !== 0,
  ]

  switch (method) {
    case 'pickFile':
      return Promise.resolve({
        cancelled: false,
        file: {
          path: 'C:\\demo\\employees.parquet',
          fileSize: 1048576,
          rowGroupCount: 4,
          totalRowCount: 10000,
          schema: {
            columns: mockColumns.map((name, i) => ({
              name,
              path: name,
              dataType: mockTypes[i],
              clrType: mockTypes[i] === 'String' ? 'String' : mockTypes[i] === 'Boolean' ? 'Boolean' : 'Int64',
              isNullable: i > 0,
              isRepeated: false,
            })),
          },
          isEncrypted: false,
          fileFormat: 'PAR1',
        },
      } as T)

    case 'getMetadata':
      return Promise.resolve({
        file: {
          path: 'C:\\demo\\employees.parquet',
          fileSize: 1048576,
          rowGroupCount: 4,
          totalRowCount: 10000,
          schema: {
            columns: mockColumns.map((name, i) => ({
              name, path: name, dataType: mockTypes[i], clrType: mockTypes[i],
              isNullable: i > 0, isRepeated: false,
            })),
          },
          isEncrypted: false,
          fileFormat: 'PAR1',
        },
        rowGroups: Array.from({ length: 4 }, (_, rg) => ({
          index: rg,
          rowCount: 2500,
          columns: mockColumns.map((name, i) => ({
            columnName: name,
            dataType: mockTypes[i],
            minValue: i === 0 ? rg * 2500 + 1 : null,
            maxValue: i === 0 ? (rg + 1) * 2500 : null,
            nullCount: i > 0 ? Math.floor(Math.random() * 10) : 0,
            distinctCount: null,
            compression: 'SNAPPY',
            compressedSize: 65536 + rg * 1024,
            uncompressedSize: 131072 + rg * 2048,
            hasColumnIndex: i < 4,
            hasOffsetIndex: i < 4,
          })),
        })),
        footer: {
          createdBy: 'kiloOhm.Parquet.Net v5.6.0',
          version: 2,
          footerLength: 0,
          keyValueMetadata: {
            'parquet.query.version': '0.1.0-preview.5',
            'created_at': '2026-03-15T10:30:00Z',
          },
        },
        encryption: null,
      } as T)

    case 'getData': {
      const p = _params as { offset?: number; limit?: number } | undefined
      const offset = p?.offset ?? 0
      const limit = p?.limit ?? 200
      const total = 10000
      const rows = Array.from({ length: Math.min(limit, total - offset) }, (_, i) => mockRow(offset + i))
      return Promise.resolve({
        columns: mockColumns,
        dataTypes: mockTypes,
        rows,
        offset,
        limit,
        totalRows: total,
      } as T)
    }

    case 'executeQuery': {
      const req = _params as { predicates: QueryPredicate[]; offset?: number; limit?: number } | undefined
      const predicates = req?.predicates ?? []
      const total = 10000
      const matched = predicates.length > 0 ? Math.floor(total * 0.3) : total
      const rows = Array.from({ length: Math.min(req?.limit ?? 200, matched) }, (_, i) => mockRow(i))
      return Promise.resolve({
        plan: {
          totalRowGroups: 4,
          selectedRowGroups: predicates.length > 0 ? 2 : 4,
          skippedRowGroups: predicates.length > 0 ? 2 : 0,
          decisions: Array.from({ length: 4 }, (_, rg) => ({
            index: rg,
            shouldRead: predicates.length === 0 || rg < 2,
            reason: predicates.length === 0
              ? 'No predicates — read all row groups.'
              : rg < 2 ? `${predicates[0]!.column}: value in range — READ` : `${predicates[0]!.column}: out of range — SKIP`,
            rowCount: 2500,
          })),
          totalRows: total,
          candidateRows: predicates.length > 0 ? 5000 : total,
          matchedRows: matched,
          executionMs: 12.34,
        },
        data: {
          columns: mockColumns,
          dataTypes: mockTypes,
          rows,
          offset: req?.offset ?? 0,
          limit: req?.limit ?? 200,
          totalRows: matched,
        },
      } as T)
    }

    case 'getQueryPlan': {
      const qp = _params as { predicates: QueryPredicate[] } | undefined
      const preds = qp?.predicates ?? []
      return Promise.resolve({
        totalRowGroups: 4,
        selectedRowGroups: preds.length > 0 ? 2 : 4,
        skippedRowGroups: preds.length > 0 ? 2 : 0,
        decisions: Array.from({ length: 4 }, (_, rg) => ({
          index: rg,
          shouldRead: preds.length === 0 || rg < 2,
          reason: preds.length === 0
            ? 'No predicates — read all row groups.'
            : rg < 2 ? 'value in statistics range — READ' : 'value outside statistics range — SKIP',
          rowCount: 2500,
        })),
        totalRows: 10000,
        candidateRows: preds.length > 0 ? 5000 : 10000,
        matchedRows: -1,
        executionMs: 0.45,
      } as T)
    }

    case 'getIndices':
      return Promise.resolve({
        customIndices: [
          {
            columnPath: 'city',
            indexType: 'Bitmap',
            description: 'Low-cardinality equality index. Stores a bitmap of which row groups contain each distinct value.',
            acceleratedOperations: ['= (equality)', '!= (inequality)'],
          },
          {
            columnPath: 'id',
            indexType: 'Hash',
            description: 'Hash-bucket index using FNV-1a. Maps values to hash buckets to prune row groups on equality lookups.',
            acceleratedOperations: ['= (equality)'],
          },
        ],
        builtinInfo: mockColumns.map((name, i) => ({
          columnPath: name,
          hasStatistics: i < 5,
          hasBloomFilter: i === 0 || i === 3,
          hasPageIndex: i < 3,
          sortOrder: i === 0 ? 'ASC' : null,
        })),
        sortingColumns: ['id ASC'],
      } as T)

    default:
      return Promise.reject(new Error(`Mock: unknown method '${method}'`))
  }
}

export const bridge = {
  pickFile: () => invoke<PickFileResult>('pickFile'),
  openFile: (path: string, encryption?: EncryptionConfig) =>
    invoke<ParquetFileInfo>('openFile', { path, encryption }),
  getSchema: () => invoke<{ columns: unknown[] }>('getSchema'),
  getMetadata: () => invoke<FileMetadataInfo>('getMetadata'),
  getIndices: () => invoke<IndicesInfo>('getIndices'),
  getData: (offset = 0, limit = 500) => invoke<DataPage>('getData', { offset, limit }),
  executeQuery: (predicates: QueryPredicate[], offset = 0, limit = 200) =>
    invoke<QueryResult>('executeQuery', { predicates, offset, limit }),
  getQueryPlan: (predicates: QueryPredicate[]) =>
    invoke<QueryPlan>('getQueryPlan', { predicates }),
}
