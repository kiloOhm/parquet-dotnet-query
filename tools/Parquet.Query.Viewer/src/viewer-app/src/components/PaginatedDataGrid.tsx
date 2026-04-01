import { useCallback, useEffect, useState } from 'react'
import type { DataPage } from '@/api/types'
import { DataGrid } from '@/components/DataGrid'
import { useError } from '@/components/ErrorDialog'
import { Button } from '@/components/ui/button'
import { formatNumber } from '@/lib/utils'
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Loader2 } from 'lucide-react'

interface PaginatedDataGridProps {
  /** Fetch a page of data. Called on mount and when the user navigates. */
  fetchPage: (offset: number, limit: number) => Promise<DataPage>
  /** Number of rows per page. */
  pageSize: number
  /** Shown when there is no data. */
  emptyMessage?: string
  /** Reset pagination and refetch when this key changes (e.g. file path or query identity). */
  resetKey?: unknown
}

export function PaginatedDataGrid({ fetchPage, pageSize, emptyMessage, resetKey }: PaginatedDataGridProps) {
  const [data, setData] = useState<DataPage | null>(null)
  const [offset, setOffset] = useState(0)
  const [loading, setLoading] = useState(false)
  const showError = useError()

  const load = useCallback(async (newOffset: number) => {
    setLoading(true)
    try {
      const page = await fetchPage(newOffset, pageSize)
      setData(page)
      setOffset(newOffset)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      showError('Failed to fetch data', msg)
    } finally {
      setLoading(false)
    }
  }, [fetchPage, pageSize, showError])

  // Reset on key change (new file, new query, etc.)
  useEffect(() => {
    if (resetKey !== undefined) {
      void load(0)
    }
  }, [resetKey, load])

  const totalRows = data?.totalRows ?? 0
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))
  const currentPage = Math.floor(offset / pageSize) + 1

  return (
    <div className="flex flex-col flex-1 min-h-0">
      {/* Pagination toolbar */}
      <div className="flex items-center justify-between px-3 py-2 border-b bg-muted/30 shrink-0">
        <div className="text-sm text-muted-foreground">
          {data ? (
            <>
              Showing {formatNumber(offset + 1)}&ndash;{formatNumber(Math.min(offset + pageSize, totalRows))} of{' '}
              {formatNumber(totalRows)} rows
            </>
          ) : (
            emptyMessage ?? 'No data'
          )}
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="outline" size="icon" className="h-8 w-8"
            disabled={offset === 0 || loading}
            onClick={() => void load(0)}
          >
            <ChevronsLeft className="h-4 w-4" />
          </Button>
          <Button
            variant="outline" size="icon" className="h-8 w-8"
            disabled={offset === 0 || loading}
            onClick={() => void load(Math.max(0, offset - pageSize))}
          >
            <ChevronLeft className="h-4 w-4" />
          </Button>
          <span className="text-sm px-2 text-muted-foreground tabular-nums">
            Page {currentPage} / {totalPages}
          </span>
          <Button
            variant="outline" size="icon" className="h-8 w-8"
            disabled={offset + pageSize >= totalRows || loading}
            onClick={() => void load(offset + pageSize)}
          >
            <ChevronRight className="h-4 w-4" />
          </Button>
          <Button
            variant="outline" size="icon" className="h-8 w-8"
            disabled={offset + pageSize >= totalRows || loading}
            onClick={() => void load((totalPages - 1) * pageSize)}
          >
            <ChevronsRight className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Grid */}
      <div className="relative flex-1 min-h-0 flex flex-col">
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-background/60 z-20">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        )}
        <DataGrid data={data} emptyMessage={emptyMessage} />
      </div>
    </div>
  )
}
