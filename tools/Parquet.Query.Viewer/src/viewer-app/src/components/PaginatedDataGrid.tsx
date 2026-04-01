import { useCallback, useEffect, useRef, useState } from 'react'
import type { DataPage } from '@/api/types'
import { DataGrid } from '@/components/DataGrid'
import { useError } from '@/components/ErrorDialog'
import { formatNumber } from '@/lib/utils'
import { Loader2 } from 'lucide-react'

const BATCH_SIZE = 500

interface PaginatedDataGridProps {
  /** Fetch a page of data. Called on mount and as the user scrolls. */
  fetchPage: (offset: number, limit: number) => Promise<DataPage>
  /** Shown when there is no data. */
  emptyMessage?: string
  /** Reset and refetch when this key changes (e.g. file path or query identity). */
  resetKey?: unknown
}

export function PaginatedDataGrid({ fetchPage, emptyMessage, resetKey }: PaginatedDataGridProps) {
  // Accumulated data across all fetched batches
  const [columns, setColumns] = useState<string[]>([])
  const [dataTypes, setDataTypes] = useState<string[]>([])
  const [rows, setRows] = useState<unknown[][]>([])
  const [totalRows, setTotalRows] = useState(0)
  const [initialLoading, setInitialLoading] = useState(false)
  const showError = useError()
  const loadingRef = useRef(false)
  const loadedCountRef = useRef(0)

  // Keep ref in sync with rows length
  loadedCountRef.current = rows.length

  // Build a synthetic DataPage from accumulated state
  const data: DataPage | null = columns.length > 0
    ? { columns, dataTypes, rows, offset: 0, limit: rows.length, totalRows }
    : null

  const hasMore = rows.length < totalRows

  const loadNext = useCallback(async () => {
    if (loadingRef.current) return
    loadingRef.current = true
    try {
      const page = await fetchPage(loadedCountRef.current, BATCH_SIZE)
      setColumns(page.columns)
      setDataTypes(page.dataTypes)
      setRows(prev => [...prev, ...page.rows])
      setTotalRows(page.totalRows)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      showError('Failed to fetch data', msg)
    } finally {
      loadingRef.current = false
    }
  }, [fetchPage, showError])

  // Reset on key change
  useEffect(() => {
    if (resetKey === undefined) return
    setRows([])
    setColumns([])
    setDataTypes([])
    setTotalRows(0)
    loadingRef.current = false
    setInitialLoading(true)
    fetchPage(0, BATCH_SIZE)
      .then(page => {
        setColumns(page.columns)
        setDataTypes(page.dataTypes)
        setRows(page.rows)
        setTotalRows(page.totalRows)
      })
      .catch(err => {
        const msg = err instanceof Error ? err.message : String(err)
        showError('Failed to fetch data', msg)
      })
      .finally(() => setInitialLoading(false))
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resetKey])

  const handleLoadMore = useCallback(() => {
    void loadNext()
  }, [loadNext])

  return (
    <div className="flex flex-col flex-1 min-h-0">
      {/* Status bar */}
      <div className="flex items-center justify-between px-3 py-1.5 border-b bg-muted/30 shrink-0">
        <div className="text-xs text-muted-foreground">
          {initialLoading ? (
            <span className="inline-flex items-center gap-1.5">
              <Loader2 className="h-3 w-3 animate-spin" /> Loading...
            </span>
          ) : data ? (
            <>
              {formatNumber(rows.length)} of {formatNumber(totalRows)} rows loaded
            </>
          ) : (
            emptyMessage ?? 'No data'
          )}
        </div>
      </div>

      {/* Grid */}
      <DataGrid
        data={data}
        emptyMessage={emptyMessage}
        onLoadMore={handleLoadMore}
        hasMore={hasMore}
      />
    </div>
  )
}
