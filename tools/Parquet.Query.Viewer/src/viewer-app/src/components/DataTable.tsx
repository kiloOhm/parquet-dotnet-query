import { useCallback, useEffect, useState } from 'react'
import { bridge } from '@/api/bridge'
import type { DataPage, ParquetFileInfo } from '@/api/types'
import { useError } from '@/components/ErrorDialog'
import { DataGrid } from '@/components/DataGrid'
import { Button } from '@/components/ui/button'
import { formatNumber } from '@/lib/utils'
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Loader2 } from 'lucide-react'

const PAGE_SIZE = 500

interface DataTableProps {
  fileInfo: ParquetFileInfo | null
}

export function DataTable({ fileInfo }: DataTableProps) {
  const [data, setData] = useState<DataPage | null>(null)
  const [loading, setLoading] = useState(false)
  const [offset, setOffset] = useState(0)
  const showError = useError()

  const fetchData = useCallback(async (newOffset: number) => {
    if (!fileInfo) return
    setLoading(true)
    try {
      const page = await bridge.getData(newOffset, PAGE_SIZE)
      setData(page)
      setOffset(newOffset)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      showError('Failed to fetch data', msg)
    } finally {
      setLoading(false)
    }
  }, [fileInfo])

  useEffect(() => {
    if (fileInfo) {
      void fetchData(0)
    } else {
      setData(null)
      setOffset(0)
    }
  }, [fileInfo, fetchData])

  if (!fileInfo) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        Open a file to view data
      </div>
    )
  }

  const totalRows = data?.totalRows ?? 0
  const totalPages = Math.ceil(totalRows / PAGE_SIZE)
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="flex items-center justify-between px-3 py-2 border-b bg-muted/30">
        <div className="text-sm text-muted-foreground">
          {data ? (
            <>
              Showing {formatNumber(offset + 1)}&ndash;{formatNumber(Math.min(offset + PAGE_SIZE, totalRows))} of{' '}
              {formatNumber(totalRows)} rows
            </>
          ) : (
            'Loading...'
          )}
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="outline" size="icon" className="h-8 w-8"
            disabled={offset === 0 || loading}
            onClick={() => void fetchData(0)}
          >
            <ChevronsLeft className="h-4 w-4" />
          </Button>
          <Button
            variant="outline" size="icon" className="h-8 w-8"
            disabled={offset === 0 || loading}
            onClick={() => void fetchData(Math.max(0, offset - PAGE_SIZE))}
          >
            <ChevronLeft className="h-4 w-4" />
          </Button>
          <span className="text-sm px-2 text-muted-foreground">
            Page {currentPage} / {totalPages}
          </span>
          <Button
            variant="outline" size="icon" className="h-8 w-8"
            disabled={offset + PAGE_SIZE >= totalRows || loading}
            onClick={() => void fetchData(offset + PAGE_SIZE)}
          >
            <ChevronRight className="h-4 w-4" />
          </Button>
          <Button
            variant="outline" size="icon" className="h-8 w-8"
            disabled={offset + PAGE_SIZE >= totalRows || loading}
            onClick={() => void fetchData((totalPages - 1) * PAGE_SIZE)}
          >
            <ChevronsRight className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Table */}
      {loading && (
        <div className="absolute inset-0 flex items-center justify-center bg-background/60 z-20">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      )}
      <DataGrid data={data} emptyMessage="Open a file to view data" />
    </div>
  )
}
