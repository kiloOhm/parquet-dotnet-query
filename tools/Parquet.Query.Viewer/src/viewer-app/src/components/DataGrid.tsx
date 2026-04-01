import { useEffect, useMemo, useRef, useState } from 'react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { useVirtualizer } from '@tanstack/react-virtual'
import type { DataPage } from '@/api/types'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Braces, Loader2 } from 'lucide-react'

type RowData = Record<string, unknown>

function isComplex(val: unknown): val is object {
  return val !== null && typeof val === 'object'
}

function formatComplex(val: unknown): string {
  try {
    return JSON.stringify(val, null, 2)
  } catch {
    return String(val)
  }
}

function complexLabel(val: object): string {
  if (Array.isArray(val)) return `[${val.length} items]`
  const keys = Object.keys(val)
  if (keys.length === 0) return '{}'
  return `{${keys.length} keys}`
}

interface DataGridProps {
  data: DataPage | null
  emptyMessage?: string
  /** Called when the user scrolls near the end of loaded rows and more data is available. */
  onLoadMore?: () => void
  /** Whether more rows can be fetched beyond those currently in `data`. */
  hasMore?: boolean
}

export function DataGrid({ data, emptyMessage, onLoadMore, hasMore }: DataGridProps) {
  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({})
  const [inspectValue, setInspectValue] = useState<{ column: string; value: unknown } | null>(null)
  const parentRef = useRef<HTMLDivElement>(null)
  const loadingRef = useRef(false)

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
          if (isComplex(val)) {
            return (
              <button
                className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs font-mono bg-muted hover:bg-muted/80 text-muted-foreground cursor-pointer border border-border"
                onClick={() => setInspectValue({ column: col, value: val })}
              >
                <Braces className="h-3 w-3 shrink-0" />
                {complexLabel(val)}
              </button>
            )
          }
          return <span>{String(val)}</span>
        },
        size: 150,
        minSize: 60,
        maxSize: 800,
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
    columnResizeMode: 'onChange',
    state: { columnSizing },
    onColumnSizingChange: setColumnSizing,
  })

  const tableRows = table.getRowModel().rows
  // Add one sentinel row for the loading indicator when more data is available
  const virtualRowCount = tableRows.length + (hasMore ? 1 : 0)

  const rowVirtualizer = useVirtualizer({
    count: virtualRowCount,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 32,
    overscan: 20,
  })

  // Trigger onLoadMore when the user scrolls near the end of loaded rows
  const virtualItems = rowVirtualizer.getVirtualItems()
  const lastItem = virtualItems[virtualItems.length - 1]
  useEffect(() => {
    if (!lastItem || !onLoadMore || !hasMore) return
    if (lastItem.index >= tableRows.length - 1 && !loadingRef.current) {
      loadingRef.current = true
      onLoadMore()
    }
  }, [lastItem?.index, tableRows.length, onLoadMore, hasMore])

  // Reset the loading guard when row count grows (fetch completed)
  useEffect(() => {
    loadingRef.current = false
  }, [tableRows.length])

  if (!data || data.rows.length === 0) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground text-sm">
        {emptyMessage ?? 'No data to display.'}
      </div>
    )
  }

  const headers = table.getHeaderGroups()[0]?.headers ?? []
  const totalWidth = headers.reduce((sum, h) => sum + h.getSize(), 0)

  return (
    <>
      <div ref={parentRef} className="flex-1 overflow-auto relative">
        <table className="border-collapse" style={{ width: totalWidth }}>
          <thead className="block sticky top-0 z-10">
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id} style={{ width: totalWidth }} className="block">
                {hg.headers.map((header) => {
                  const w = header.getSize()
                  return (
                    <th
                      key={header.id}
                      style={{ width: w, maxWidth: w, minWidth: w }}
                      className="relative bg-muted px-3 py-2 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider border-b border-r select-none inline-block box-border"
                    >
                      {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                      <div
                        onMouseDown={header.getResizeHandler()}
                        onTouchStart={header.getResizeHandler()}
                        className={`absolute right-0 top-0 h-full w-1.5 cursor-col-resize select-none touch-none hover:bg-primary/40 ${
                          header.column.getIsResizing() ? 'bg-primary/60' : ''
                        }`}
                      />
                    </th>
                  )
                })}
              </tr>
            ))}
          </thead>
          <tbody style={{ height: `${rowVirtualizer.getTotalSize()}px`, display: 'block', position: 'relative' }}>
            {rowVirtualizer.getVirtualItems().map((virtualRow) => {
              // Sentinel row: loading spinner
              if (virtualRow.index >= tableRows.length) {
                return (
                  <tr
                    key="sentinel"
                    style={{
                      position: 'absolute',
                      top: 0,
                      left: 0,
                      width: totalWidth,
                      height: `${virtualRow.size}px`,
                      transform: `translateY(${virtualRow.start}px)`,
                    }}
                  >
                    <td
                      style={{ width: totalWidth }}
                      className="inline-flex items-center justify-center text-muted-foreground"
                    >
                      <Loader2 className="h-4 w-4 animate-spin" />
                    </td>
                  </tr>
                )
              }
              const row = tableRows[virtualRow.index]!
              return (
                <tr
                  key={row.id}
                  style={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    width: totalWidth,
                    height: `${virtualRow.size}px`,
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                >
                  {row.getVisibleCells().map((cell) => {
                    const w = cell.column.getSize()
                    return (
                      <td
                        key={cell.id}
                        style={{ width: w, maxWidth: w, minWidth: w, height: `${virtualRow.size}px` }}
                        className="px-3 py-1.5 text-sm border-b border-r inline-block box-border"
                      >
                        <div className="truncate">
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </div>
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      <Dialog open={inspectValue !== null} onOpenChange={(open) => { if (!open) setInspectValue(null) }}>
        <DialogContent className="max-w-2xl max-h-[80vh] flex flex-col">
          <DialogHeader>
            <DialogTitle className="font-mono text-sm">{inspectValue?.column}</DialogTitle>
            <DialogDescription>Cell value</DialogDescription>
          </DialogHeader>
          <ScrollArea className="flex-1 min-h-0">
            <pre className="text-xs font-mono whitespace-pre-wrap wrap-break-word p-3 bg-muted rounded">
              {inspectValue ? formatComplex(inspectValue.value) : ''}
            </pre>
          </ScrollArea>
        </DialogContent>
      </Dialog>
    </>
  )
}
