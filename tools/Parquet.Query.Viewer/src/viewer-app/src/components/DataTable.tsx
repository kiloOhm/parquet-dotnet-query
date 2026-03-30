import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { useVirtualizer } from '@tanstack/react-virtual'
import { bridge } from '@/api/bridge'
import type { DataPage, ParquetFileInfo } from '@/api/types'
import { Button } from '@/components/ui/button'
import { formatNumber } from '@/lib/utils'
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Loader2 } from 'lucide-react'

const PAGE_SIZE = 500

interface DataTableProps {
  fileInfo: ParquetFileInfo | null
}

type RowData = Record<string, unknown>

export function DataTable({ fileInfo }: DataTableProps) {
  const [data, setData] = useState<DataPage | null>(null)
  const [loading, setLoading] = useState(false)
  const [offset, setOffset] = useState(0)
  const parentRef = useRef<HTMLDivElement>(null)

  const fetchData = useCallback(async (newOffset: number) => {
    if (!fileInfo) return
    setLoading(true)
    try {
      const page = await bridge.getData(newOffset, PAGE_SIZE)
      setData(page)
      setOffset(newOffset)
    } catch (err) {
      console.error('Failed to fetch data:', err)
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

  const columnHelper = createColumnHelper<RowData>()

  const columns = useMemo(() => {
    if (!data) return []
    return data.columns.map((col, i) =>
      columnHelper.accessor(col, {
        id: col,
        header: () => (
          <div className="flex flex-col">
            <span className="font-semibold">{col}</span>
            <span className="text-[10px] font-normal text-muted-foreground normal-case tracking-normal">
              {data.dataTypes[i]}
            </span>
          </div>
        ),
        cell: (info) => {
          const val = info.getValue()
          if (val === null || val === undefined) {
            return <span className="text-muted-foreground/50 italic">null</span>
          }
          if (typeof val === 'boolean') {
            return <span className={val ? 'text-emerald-400' : 'text-red-400'}>{String(val)}</span>
          }
          return <span>{String(val)}</span>
        },
        size: 150,
      }),
    )
  }, [data, columnHelper])

  const rows = useMemo<RowData[]>(() => {
    if (!data) return []
    return data.rows.map((row) => {
      const obj: RowData = {}
      data.columns.forEach((col, i) => {
        obj[col] = row[i]
      })
      return obj
    })
  }, [data])

  const table = useReactTable({
    data: rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  const tableRows = table.getRowModel().rows

  const rowVirtualizer = useVirtualizer({
    count: tableRows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 32,
    overscan: 20,
  })

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
      <div ref={parentRef} className="flex-1 overflow-auto relative">
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-background/60 z-20">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        )}
        <table className="w-full border-collapse" style={{ minWidth: (data?.columns.length ?? 1) * 150 }}>
          <thead className="virtual-table-header">
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id}>
                {hg.headers.map((header) => (
                  <th
                    key={header.id}
                    style={{ width: header.getSize() }}
                    className="bg-muted px-3 py-2 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider border-b border-r sticky top-0 z-10"
                  >
                    {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody
            className="virtual-table-body"
            style={{ height: `${rowVirtualizer.getTotalSize()}px`, position: 'relative' }}
          >
            {rowVirtualizer.getVirtualItems().map((virtualRow) => {
              const row = tableRows[virtualRow.index]!
              return (
                <tr
                  key={row.id}
                  style={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    width: '100%',
                    height: `${virtualRow.size}px`,
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                >
                  {row.getVisibleCells().map((cell) => (
                    <td
                      key={cell.id}
                      style={{ width: cell.column.getSize() }}
                      className="px-3 py-1.5 text-sm border-b border-r truncate inline-block"
                    >
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
