import type {
  DataPage,
  EncryptionConfig,
  FileMetadataInfo,
  IndexEntriesPage,
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type PushHandler = (data: any) => void

const pending = new Map<string, PendingRequest>()
const pushHandlers = new Map<string, Set<PushHandler>>()
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
        id?: string; method?: string; data?: unknown; result?: unknown; error?: string
      }

      // Push message from native (no id, has method)
      if (msg.method && !msg.id) {
        const handlers = pushHandlers.get(msg.method)
        if (handlers) {
          for (const handler of handlers) handler(msg.data)
        }
        return
      }

      // Response to a pending request
      const req = msg.id ? pending.get(msg.id) : undefined
      if (!req) return
      pending.delete(msg.id!)
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
            stats: { payloadBytes: 312, distinctValueCount: 6, entryCount: 6 },
          },
          {
            columnPath: 'name',
            indexType: 'Lucene',
            description: 'Full-text search index with tokenization and optional fuzzy matching.',
            acceleratedOperations: ['LuceneMatch (term search)', 'LuceneFuzzy (fuzzy matching)'],
            stats: { payloadBytes: 2048, termCount: 8, entryCount: 8 },
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

    case 'getIndexEntries': {
      const ep = _params as { columnPath?: string; indexType?: string; offset?: number; limit?: number; filter?: string } | undefined
      const allEntries: Record<string, { key: string; rowGroups: number[] }[]> = {
        'Bitmap:city': [
          { key: 'New York', rowGroups: [0, 1, 3] },
          { key: 'London', rowGroups: [0, 2] },
          { key: 'Tokyo', rowGroups: [1, 2, 3] },
          { key: 'Berlin', rowGroups: [0, 1] },
          { key: 'Paris', rowGroups: [2, 3] },
          { key: 'Sydney', rowGroups: [1] },
        ],
        'Lucene:name': [
          { key: 'alice', rowGroups: [0, 2] },
          { key: 'bob', rowGroups: [0, 1, 3] },
          { key: 'charlie', rowGroups: [1] },
          { key: 'diana', rowGroups: [1, 2] },
          { key: 'eve', rowGroups: [2, 3] },
          { key: 'frank', rowGroups: [0, 3] },
          { key: 'grace', rowGroups: [3] },
          { key: 'hank', rowGroups: [0, 1, 2, 3] },
        ],
      }
      const cacheKey = `${ep?.indexType ?? ''}:${ep?.columnPath ?? ''}`
      let entries = allEntries[cacheKey] ?? []
      if (ep?.filter) {
        const lower = ep.filter.toLowerCase()
        entries = entries.filter(e => e.key.toLowerCase().includes(lower))
      }
      const off = ep?.offset ?? 0
      const lim = ep?.limit ?? 100
      return Promise.resolve({
        entries: entries.slice(off, off + lim),
        totalEntries: entries.length,
        offset: off,
        limit: lim,
      } as T)
    }

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
  getIndexEntries: (columnPath: string, indexType: string, offset = 0, limit = 100, filter?: string) =>
    invoke<IndexEntriesPage>('getIndexEntries', { columnPath, indexType, offset, limit, filter }),
  getData: (offset = 0, limit = 500) => invoke<DataPage>('getData', { offset, limit }),
  executeQuery: (predicates: QueryPredicate[], offset = 0, limit = 200) =>
    invoke<QueryResult>('executeQuery', { predicates, offset, limit }),
  getQueryPlan: (predicates: QueryPredicate[]) =>
    invoke<QueryPlan>('getQueryPlan', { predicates }),
  /** Subscribe to push messages from the native host. Returns an unsubscribe function. */
  onPush: (method: string, handler: PushHandler): (() => void) => {
    let handlers = pushHandlers.get(method)
    if (!handlers) {
      handlers = new Set()
      pushHandlers.set(method, handlers)
    }
    handlers.add(handler)
    return () => { handlers!.delete(handler) }
  },
}
