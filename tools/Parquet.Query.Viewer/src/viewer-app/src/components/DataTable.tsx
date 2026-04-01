import { useCallback } from 'react'
import { bridge } from '@/api/bridge'
import type { ParquetFileInfo } from '@/api/types'
import { PaginatedDataGrid } from '@/components/PaginatedDataGrid'

const PAGE_SIZE = 500

interface DataTableProps {
  fileInfo: ParquetFileInfo | null
}

export function DataTable({ fileInfo }: DataTableProps) {
  const fetchPage = useCallback(
    (offset: number, limit: number) => bridge.getData(offset, limit),
    [],
  )

  if (!fileInfo) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        Open a file to view data
      </div>
    )
  }

  return (
    <PaginatedDataGrid
      fetchPage={fetchPage}
      pageSize={PAGE_SIZE}
      emptyMessage="Open a file to view data"
      resetKey={fileInfo.path}
    />
  )
}
