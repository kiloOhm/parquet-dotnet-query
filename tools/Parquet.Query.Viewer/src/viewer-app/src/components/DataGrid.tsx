import { forwardRef, useEffect, useImperativeHandle, useMemo, useRef, useState } from 'react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { useVirtualizer } from '@tanstack/react-virtual'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Braces } from 'lucide-react'

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

export interface DataGridHandle {
  /** Scroll to a specific row index. */
  scrollToRow: (index: number) => void
}

interface DataGridProps {
  /** Column names. */
  columns: string[]
  /** Column data types (parallel to columns). */
  dataTypes: string[]
  /** Total number of rows in the dataset. */
  totalRows: number
  /** Retrieve a row by absolute index. Returns null if not yet loaded. */
  getRow: (index: number) => unknown[] | null
  /** Opaque counter — changing this forces a re-render so newly cached rows appear. */
  cacheVersion?: number
  /** Called when the visible range changes (after scroll settles). */
  onVisibleRangeChange?: (startIndex: number, endIndex: number) => void
  /** Empty state message. */
  emptyMessage?: string
}

export const DataGrid = forwardRef<DataGridHandle, DataGridProps>(function DataGrid({
  columns: colNames,
  dataTypes,
  totalRows,
  getRow,
  cacheVersion: _cacheVersion,
  onVisibleRangeChange,
  emptyMessage,
}, ref) {
  // _cacheVersion is intentionally unused — its presence as a prop
  // forces React to re-render this component when the cache updates.
  void _cacheVersion
  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({})
  const [inspectValue, setInspectValue] = useState<{ column: string; value: unknown } | null>(null)
  const parentRef = useRef<HTMLDivElement>(null)

  const columnHelper = createColumnHelper<RowData>()

  // Column definitions for react-table (used for headers + column sizing only)
  const columns = useMemo(() => {
    return colNames.map((col, i) =>
      columnHelper.accessor(col, {
        id: col,
        header: () => (
          <div className="flex flex-col">
            <span className="font-semibold">{col}</span>
            <span className="text-[10px] font-normal text-muted-foreground normal-case tracking-normal">
              {dataTypes[i]}
            </span>
          </div>
        ),
        // Cell renderer is not used by react-table (we render rows manually),
        // but required by the column definition type.
        cell: () => null,
        size: 150,
        minSize: 60,
        maxSize: 800,
      }),
    )
  }, [colNames, dataTypes, columnHelper])

  // Feed react-table an empty data array — we only use it for headers and column sizing.
  const table = useReactTable({
    data: [] as RowData[],
    columns,
    getCoreRowModel: getCoreRowModel(),
    columnResizeMode: 'onChange',
    state: { columnSizing },
    onColumnSizingChange: setColumnSizing,
  })

  const rowVirtualizer = useVirtualizer({
    count: totalRows,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 32,
    overscan: 20,
  })

  useImperativeHandle(ref, () => ({
    scrollToRow: (index: number) => {
      rowVirtualizer.scrollToIndex(index, { align: 'start' })
    },
  }), [rowVirtualizer])

  // Report visible range whenever the virtualizer recalculates (scroll, resize, initial measure).
  // Using primitive deps so the effect only fires when the range actually changes.
  const virtualItems = rowVirtualizer.getVirtualItems()
  const rangeStart = virtualItems[0]?.index ?? 0
  const rangeEnd = virtualItems[virtualItems.length - 1]?.index ?? 0

  useEffect(() => {
    if (!onVisibleRangeChange || virtualItems.length === 0) return
    onVisibleRangeChange(rangeStart, rangeEnd)
  }, [rangeStart, rangeEnd, onVisibleRangeChange, virtualItems.length])

  if (totalRows === 0) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground text-sm">
        {emptyMessage ?? 'No data to display.'}
      </div>
    )
  }

  const headers = table.getHeaderGroups()[0]?.headers ?? []
  const totalWidth = headers.reduce((sum, h) => sum + h.getSize(), 0)

  // Render a single cell value
  const renderCellValue = (col: string, val: unknown) => {
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
  }

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
              const rowData = getRow(virtualRow.index)

              // Placeholder shimmer for rows not yet loaded
              if (rowData === null) {
                return (
                  <tr
                    key={`ph-${virtualRow.index}`}
                    style={{
                      position: 'absolute',
                      top: 0,
                      left: 0,
                      width: totalWidth,
                      height: `${virtualRow.size}px`,
                      transform: `translateY(${virtualRow.start}px)`,
                    }}
                  >
                    {headers.map((header) => {
                      const w = header.getSize()
                      return (
                        <td
                          key={header.id}
                          style={{ width: w, maxWidth: w, minWidth: w, height: `${virtualRow.size}px` }}
                          className="px-3 py-1.5 text-sm border-b border-r inline-block box-border"
                        >
                          <div className="h-3 w-2/3 bg-muted animate-pulse rounded" />
                        </td>
                      )
                    })}
                  </tr>
                )
              }

              return (
                <tr
                  key={virtualRow.index}
                  style={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    width: totalWidth,
                    height: `${virtualRow.size}px`,
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                >
                  {headers.map((header, colIdx) => {
                    const w = header.getSize()
                    const val = rowData[colIdx]
                    return (
                      <td
                        key={header.id}
                        style={{ width: w, maxWidth: w, minWidth: w, height: `${virtualRow.size}px` }}
                        className="px-3 py-1.5 text-sm border-b border-r inline-block box-border"
                      >
                        <div className="truncate">
                          {renderCellValue(colNames[colIdx]!, val)}
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
})
