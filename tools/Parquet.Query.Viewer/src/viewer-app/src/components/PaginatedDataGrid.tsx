import { useCallback, useEffect, useRef, useState } from 'react'
import type { DataPage } from '@/api/types'
import { DataGrid, type DataGridHandle } from '@/components/DataGrid'
import { useError } from '@/components/ErrorDialog'
import { useSparseRowCache, CHUNK_SIZE } from '@/hooks/useSparseRowCache'
import { formatNumber } from '@/lib/utils'
import { Loader2 } from 'lucide-react'

const DEBOUNCE_MS = 150

interface PaginatedDataGridProps {
  /** Fetch a page of data. Called on mount and as the user scrolls. */
  fetchPage: (offset: number, limit: number) => Promise<DataPage>
  /** Shown when there is no data. */
  emptyMessage?: string
  /** Reset and refetch when this key changes (e.g. file path or query identity). */
  resetKey?: unknown
}

export function PaginatedDataGrid({ fetchPage, emptyMessage, resetKey }: PaginatedDataGridProps) {
  const [columns, setColumns] = useState<string[]>([])
  const [dataTypes, setDataTypes] = useState<string[]>([])
  const [totalRows, setTotalRows] = useState(0)
  const [initialLoading, setInitialLoading] = useState(false)
  const [visibleStart, setVisibleStart] = useState(0)
  const [visibleEnd, setVisibleEnd] = useState(0)
  const showError = useError()
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const visibleRef = useRef({ start: 0, end: 0 })
  const gridRef = useRef<DataGridHandle>(null)
  const [goToInput, setGoToInput] = useState('')

  const { getRow, ensureRange, seedChunk, clear, version } = useSparseRowCache(fetchPage)

  // Reset on key change: clear cache, fetch the first chunk to learn schema + totalRows
  useEffect(() => {
    if (resetKey === undefined) return
    clear()
    setColumns([])
    setDataTypes([])
    setTotalRows(0)
    setVisibleStart(0)
    setVisibleEnd(0)
    visibleRef.current = { start: 0, end: 0 }
    setInitialLoading(true)

    fetchPage(0, CHUNK_SIZE)
      .then(page => {
        setColumns(page.columns)
        setDataTypes(page.dataTypes)
        setTotalRows(page.totalRows)
        seedChunk(0, page.rows)
      })
      .catch(err => {
        const msg = err instanceof Error ? err.message : String(err)
        showError('Failed to fetch data', msg)
      })
      .finally(() => setInitialLoading(false))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resetKey])

  // Keep totalRows in a ref so the debounce callback always sees the latest value
  const totalRowsRef = useRef(totalRows)
  totalRowsRef.current = totalRows

  // Stable callback — only depends on ensureRange (stable useCallback from the hook)
  const handleVisibleRangeChange = useCallback((start: number, end: number) => {
    // Deduplicate: skip if range hasn't actually changed
    const prev = visibleRef.current
    if (prev.start === start && prev.end === end) return
    visibleRef.current = { start, end }
    setVisibleStart(start)
    setVisibleEnd(end)

    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      const total = totalRowsRef.current
      if (total === 0) return
      const bufferedStart = Math.max(0, start - CHUNK_SIZE)
      const bufferedEnd = Math.min(total - 1, end + CHUNK_SIZE)
      ensureRange(bufferedStart, bufferedEnd)
    }, DEBOUNCE_MS)
  }, [ensureRange])

  // Clean up debounce timer
  useEffect(() => {
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current)
    }
  }, [])

  const handleGoToRow = () => {
    const parsed = parseInt(goToInput, 10)
    if (isNaN(parsed) || parsed < 1) return
    const clamped = Math.min(parsed, totalRows) - 1 // convert 1-based to 0-based
    gridRef.current?.scrollToRow(clamped)
    setGoToInput('')
  }

  return (
    <div className="flex flex-col flex-1 min-h-0">
      {/* Status bar */}
      <div className="flex items-center justify-between px-3 py-1.5 border-b bg-muted/30 shrink-0">
        <div className="text-xs text-muted-foreground">
          {initialLoading ? (
            <span className="inline-flex items-center gap-1.5">
              <Loader2 className="h-3 w-3 animate-spin" /> Loading...
            </span>
          ) : totalRows > 0 ? (
            <>
              Rows {formatNumber(visibleStart + 1)}&ndash;{formatNumber(Math.min(visibleEnd + 1, totalRows))} of {formatNumber(totalRows)}
            </>
          ) : (
            emptyMessage ?? 'No data'
          )}
        </div>
        {totalRows > 0 && !initialLoading && (
          <form
            className="flex items-center gap-1.5"
            onSubmit={(e) => { e.preventDefault(); handleGoToRow() }}
          >
            <label htmlFor="go-to-row" className="text-xs text-muted-foreground whitespace-nowrap">
              Go to row
            </label>
            <input
              id="go-to-row"
              type="text"
              inputMode="numeric"
              className="h-6 w-20 rounded border border-border bg-background px-1.5 text-xs text-foreground outline-none focus:border-ring focus:ring-1 focus:ring-ring/50"
              placeholder={`1–${formatNumber(totalRows)}`}
              value={goToInput}
              onChange={(e) => setGoToInput(e.target.value.replace(/[^\d]/g, ''))}
            />
          </form>
        )}
      </div>

      {/* Grid */}
      <DataGrid
        ref={gridRef}
        columns={columns}
        dataTypes={dataTypes}
        totalRows={totalRows}
        getRow={getRow}
        cacheVersion={version}
        onVisibleRangeChange={handleVisibleRangeChange}
        emptyMessage={emptyMessage}
      />
    </div>
  )
}
