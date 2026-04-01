import { useCallback, useEffect, useRef, useState } from 'react'
import { useVirtualizer } from '@tanstack/react-virtual'
import { bridge } from '@/api/bridge'
import type { ColumnIndexInfo, IndexEntry, IndicesInfo, ParquetFileInfo } from '@/api/types'
import { useError } from '@/components/ErrorDialog'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import {
  ArrowDown,
  ArrowUp,
  BarChart3,
  Check,
  ChevronDown,
  ChevronRight,
  Database,
  Hash,
  Loader2,
  Minus,
  Search,
} from 'lucide-react'

interface IndicesViewerProps {
  fileInfo: ParquetFileInfo | null
}

const INDEX_ICONS: Record<string, typeof Database> = {
  Bitmap: Database,
  Lucene: Search,
}

const INDEX_COLORS: Record<string, string> = {
  Bitmap: 'bg-blue-500/10 text-blue-500 border-blue-500/20',
  Lucene: 'bg-emerald-500/10 text-emerald-500 border-emerald-500/20',
}

const RG_COLORS = [
  'bg-blue-500',
  'bg-emerald-500',
  'bg-amber-500',
  'bg-rose-500',
  'bg-violet-500',
  'bg-cyan-500',
  'bg-orange-500',
  'bg-pink-500',
]

function BoolCell({ value }: { value: boolean }) {
  return value ? (
    <Check className="h-4 w-4 text-emerald-500 mx-auto" />
  ) : (
    <Minus className="h-4 w-4 text-muted-foreground/30 mx-auto" />
  )
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function StatRow({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="flex justify-between items-baseline">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="text-xs font-mono font-medium">{value}</span>
    </div>
  )
}

function RowGroupDots({ rowGroups, maxRowGroup }: { rowGroups: number[]; maxRowGroup: number }) {
  const set = new Set(rowGroups)
  return (
    <div className="flex gap-0.5 items-center">
      {Array.from({ length: maxRowGroup + 1 }, (_, i) => (
        <div
          key={i}
          className={`w-2.5 h-2.5 rounded-sm ${set.has(i) ? RG_COLORS[i % RG_COLORS.length] : 'bg-muted'}`}
          title={`RG ${i}${set.has(i) ? '' : ' (absent)'}`}
        />
      ))}
    </div>
  )
}

const PAGE_SIZE = 100

function EntryBrowser({ columnPath, indexType, totalEntryCount, maxRowGroup }: {
  columnPath: string
  indexType: string
  totalEntryCount: number
  maxRowGroup: number
}) {
  const [filter, setFilter] = useState('')
  const [debouncedFilter, setDebouncedFilter] = useState('')
  const [entries, setEntries] = useState<IndexEntry[]>([])
  const [totalEntries, setTotalEntries] = useState(totalEntryCount)
  const [loading, setLoading] = useState(false)
  const [hasMore, setHasMore] = useState(true)
  const parentRef = useRef<HTMLDivElement>(null)
  const loadingRef = useRef(false)

  // Debounce filter input
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedFilter(filter), 250)
    return () => clearTimeout(timer)
  }, [filter])

  // Reset and load first page when filter changes
  useEffect(() => {
    setEntries([])
    setHasMore(true)
    loadPage(0, debouncedFilter, true)
  }, [debouncedFilter, columnPath, indexType])

  const loadPage = useCallback((offset: number, filterValue: string, replace: boolean) => {
    if (loadingRef.current) return
    loadingRef.current = true
    setLoading(true)
    bridge.getIndexEntries(columnPath, indexType, offset, PAGE_SIZE, filterValue || undefined)
      .then((page) => {
        setEntries(prev => replace ? page.entries : [...prev, ...page.entries])
        setTotalEntries(page.totalEntries)
        setHasMore(offset + page.entries.length < page.totalEntries)
      })
      .catch(() => setHasMore(false))
      .finally(() => {
        loadingRef.current = false
        setLoading(false)
      })
  }, [columnPath, indexType])

  const rowVirtualizer = useVirtualizer({
    count: entries.length + (hasMore ? 1 : 0),
    getScrollElement: () => parentRef.current,
    estimateSize: () => 28,
    overscan: 10,
  })

  // Load next page when the sentinel row becomes visible
  const virtualItems = rowVirtualizer.getVirtualItems()
  const lastItem = virtualItems[virtualItems.length - 1]
  useEffect(() => {
    if (!lastItem) return
    if (lastItem.index >= entries.length - 1 && hasMore && !loadingRef.current) {
      loadPage(entries.length, debouncedFilter, false)
    }
  }, [lastItem?.index, entries.length, hasMore, debouncedFilter, loadPage])

  const keyLabel = indexType === 'Lucene' ? 'Term'
    : indexType === 'Bitmap' ? 'Value'
    : 'Bucket'

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <div className="relative flex-1">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground" />
          <Input
            className="h-7 pl-7 text-xs"
            placeholder={`Filter ${totalEntryCount.toLocaleString()} ${totalEntryCount === 1 ? 'entry' : 'entries'}...`}
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
        </div>
        <span className="text-[10px] text-muted-foreground whitespace-nowrap tabular-nums">
          {loading && <Loader2 className="inline h-3 w-3 animate-spin mr-1" />}
          {totalEntries === totalEntryCount
            ? `${totalEntries.toLocaleString()} entries`
            : `${totalEntries.toLocaleString()} / ${totalEntryCount.toLocaleString()}`}
        </span>
      </div>

      <div className="rounded-md border bg-card overflow-hidden">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b bg-muted/30">
              <th className="text-left px-3 py-1.5 font-medium">{keyLabel}</th>
              <th className="text-left px-3 py-1.5 font-medium">Row Groups</th>
            </tr>
          </thead>
        </table>

        <div ref={parentRef} className="max-h-[280px] overflow-auto">
          {entries.length === 0 && !loading ? (
            <div className="px-3 py-4 text-center text-muted-foreground text-xs">
              {debouncedFilter ? 'No matching entries' : 'No entries'}
            </div>
          ) : (
            <div style={{ height: `${rowVirtualizer.getTotalSize()}px`, position: 'relative' }}>
              {virtualItems.map((virtualRow) => {
                if (virtualRow.index >= entries.length) {
                  // Sentinel / loading row
                  return (
                    <div
                      key="loading"
                      style={{
                        position: 'absolute',
                        top: 0,
                        left: 0,
                        width: '100%',
                        height: `${virtualRow.size}px`,
                        transform: `translateY(${virtualRow.start}px)`,
                      }}
                      className="flex items-center justify-center text-xs text-muted-foreground"
                    >
                      <Loader2 className="h-3 w-3 animate-spin" />
                    </div>
                  )
                }
                const entry = entries[virtualRow.index]!
                return (
                  <div
                    key={entry.key}
                    style={{
                      position: 'absolute',
                      top: 0,
                      left: 0,
                      width: '100%',
                      height: `${virtualRow.size}px`,
                      transform: `translateY(${virtualRow.start}px)`,
                    }}
                    className="flex items-center border-b last:border-0 hover:bg-muted/20 text-xs"
                  >
                    <span className="px-3 py-1 font-mono truncate flex-1 min-w-0" title={entry.key}>
                      {entry.key}
                    </span>
                    <span className="px-3 py-1 shrink-0">
                      <RowGroupDots rowGroups={entry.rowGroups} maxRowGroup={maxRowGroup} />
                    </span>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      </div>

      {/* Row group legend */}
      <div className="flex flex-wrap gap-2">
        {Array.from({ length: maxRowGroup + 1 }, (_, i) => (
          <div key={i} className="flex items-center gap-1">
            <div className={`w-2 h-2 rounded-sm ${RG_COLORS[i % RG_COLORS.length]}`} />
            <span className="text-[10px] text-muted-foreground">RG {i}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function IndexCard({ idx, maxRowGroup }: { idx: ColumnIndexInfo; maxRowGroup: number }) {
  const [expanded, setExpanded] = useState(false)
  const Icon = INDEX_ICONS[idx.indexType] ?? Hash
  const colorClass = INDEX_COLORS[idx.indexType] ?? 'bg-muted text-foreground border-border'
  const stats = idx.stats
  const hasEntries = stats != null && stats.entryCount > 0

  return (
    <div className="px-4 py-3 space-y-2">
      <div className="flex items-center gap-2">
        <div className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-md text-xs font-medium border ${colorClass}`}>
          <Icon className="h-3.5 w-3.5" />
          {idx.indexType}
        </div>
        {stats && (
          <div className="flex gap-3 ml-auto text-[10px] text-muted-foreground">
            {stats.termCount != null && <span>{stats.termCount.toLocaleString()} terms</span>}
            {stats.distinctValueCount != null && <span>{stats.distinctValueCount.toLocaleString()} values</span>}
            <span>{formatBytes(stats.payloadBytes)}</span>
          </div>
        )}
      </div>

      <p className="text-xs text-muted-foreground leading-relaxed">
        {idx.description}
      </p>

      <div className="flex items-center gap-1.5 flex-wrap">
        {idx.acceleratedOperations.map((op) => (
          <Badge key={op} variant="secondary" className="text-[10px] font-mono">
            {op}
          </Badge>
        ))}
      </div>

      {hasEntries && (
        <>
          <button
            type="button"
            onClick={() => setExpanded(!expanded)}
            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
          >
            {expanded
              ? <ChevronDown className="h-3.5 w-3.5" />
              : <ChevronRight className="h-3.5 w-3.5" />
            }
            {expanded ? 'Hide' : 'Browse'} index data
          </button>

          {expanded && (
            <EntryBrowser
              columnPath={idx.columnPath}
              indexType={idx.indexType}
              totalEntryCount={stats!.entryCount}
              maxRowGroup={maxRowGroup}
            />
          )}
        </>
      )}

      {stats && !hasEntries && (
        <div className="rounded-md bg-muted/30 border border-border/50 px-3 py-2 space-y-1">
          <div className="grid grid-cols-2 gap-x-6 gap-y-1">
            {stats.termCount != null && <StatRow label="Indexed terms" value={stats.termCount.toLocaleString()} />}
            {stats.distinctValueCount != null && <StatRow label="Distinct values" value={stats.distinctValueCount.toLocaleString()} />}
            <StatRow label="Payload size" value={formatBytes(stats.payloadBytes)} />
          </div>
        </div>
      )}
    </div>
  )
}

export function IndicesViewer({ fileInfo }: IndicesViewerProps) {
  const [info, setInfo] = useState<IndicesInfo | null>(null)
  const [loading, setLoading] = useState(false)
  const showError = useError()

  useEffect(() => {
    if (!fileInfo) {
      setInfo(null)
      return
    }
    setLoading(true)
    bridge.getIndices()
      .then(setInfo)
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err)
        showError('Failed to load indices', msg)
      })
      .finally(() => setLoading(false))
  }, [fileInfo])

  if (!fileInfo) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        Open a file to view indices
      </div>
    )
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        Loading indices...
      </div>
    )
  }

  if (!info) return null

  const { customIndices, builtinInfo, sortingColumns } = info
  const hasCustom = customIndices.length > 0
  const maxRowGroup = Math.max(0, (fileInfo.rowGroupCount ?? 1) - 1)

  // Group custom indices by column
  const byColumn = new Map<string, typeof customIndices>()
  for (const idx of customIndices) {
    const list = byColumn.get(idx.columnPath) ?? []
    list.push(idx)
    byColumn.set(idx.columnPath, list)
  }

  return (
    <Tabs defaultValue="builtin" className="h-full">
      <div className="px-3 pt-2 shrink-0">
        <TabsList>
          <TabsTrigger value="builtin">
            <BarChart3 className="h-3.5 w-3.5" />
            Built-in
          </TabsTrigger>
          <TabsTrigger value="custom">
            <Hash className="h-3.5 w-3.5" />
            Custom
            {hasCustom && (
              <Badge variant="secondary" className="text-[9px] ml-1 px-1 py-0">
                {customIndices.length}
              </Badge>
            )}
          </TabsTrigger>
        </TabsList>
      </div>

      {/* Built-in Optimizations */}
      <TabsContent value="builtin" className="flex-1 min-h-0">
        <ScrollArea className="h-full">
          <div className="p-4 space-y-6">
            {/* Sorting columns summary */}
            {sortingColumns.length > 0 && (
              <div className="rounded-lg border bg-card p-4">
                <h3 className="text-sm font-semibold mb-2 flex items-center gap-2">
                  <ArrowUp className="h-4 w-4" />
                  Sort Order
                </h3>
                <div className="flex flex-wrap gap-2">
                  {sortingColumns.map((col) => {
                    const isDesc = col.includes('DESC')
                    const SortIcon = isDesc ? ArrowDown : ArrowUp
                    return (
                      <Badge key={col} variant="outline" className="text-xs font-mono gap-1">
                        <SortIcon className="h-3 w-3" />
                        {col}
                      </Badge>
                    )
                  })}
                </div>
                <p className="text-xs text-muted-foreground mt-2">
                  Data is physically sorted by these columns, enabling efficient range scans and statistics-based pruning.
                </p>
              </div>
            )}

            {builtinInfo.length > 0 && (
              <div className="space-y-3">
                <p className="text-xs text-muted-foreground">
                  Per-column metadata from the Parquet file that enables query acceleration without custom indices.
                </p>
                <div className="rounded-lg border bg-card overflow-hidden">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b bg-muted/30">
                        <th className="text-left px-4 py-2 font-medium text-xs">Column</th>
                        <th className="text-center px-3 py-2 font-medium text-xs" title="Min/max statistics for range pruning">Statistics</th>
                        <th className="text-center px-3 py-2 font-medium text-xs" title="Bloom filter for equality checks">Bloom Filter</th>
                        <th className="text-center px-3 py-2 font-medium text-xs" title="Page-level min/max index for fine-grained skipping">Page Index</th>
                        <th className="text-center px-3 py-2 font-medium text-xs">Sort</th>
                      </tr>
                    </thead>
                    <tbody>
                      {builtinInfo.map((col) => (
                        <tr key={col.columnPath} className="border-b last:border-0 hover:bg-muted/20">
                          <td className="px-4 py-2 font-mono text-xs">{col.columnPath}</td>
                          <td className="px-3 py-2 text-center">
                            <BoolCell value={col.hasStatistics} />
                          </td>
                          <td className="px-3 py-2 text-center">
                            <BoolCell value={col.hasBloomFilter} />
                          </td>
                          <td className="px-3 py-2 text-center">
                            <BoolCell value={col.hasPageIndex} />
                          </td>
                          <td className="px-3 py-2 text-center">
                            {col.sortOrder ? (
                              <Badge variant="outline" className="text-[10px] font-mono">
                                {col.sortOrder}
                              </Badge>
                            ) : (
                              <Minus className="h-4 w-4 text-muted-foreground/30 mx-auto" />
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="grid grid-cols-2 gap-3 text-xs text-muted-foreground">
                  <div>
                    <span className="font-medium text-foreground">Statistics</span> — Min/max values per column chunk. Allows skipping entire row groups when a filter value falls outside the range.
                  </div>
                  <div>
                    <span className="font-medium text-foreground">Bloom Filter</span> — Probabilistic set membership test. Accelerates equality predicates with zero false negatives.
                  </div>
                  <div>
                    <span className="font-medium text-foreground">Page Index</span> — Page-level min/max values for fine-grained row skipping within a row group.
                  </div>
                  <div>
                    <span className="font-medium text-foreground">Sort Order</span> — Physically sorted data enables binary search and tighter statistics bounds.
                  </div>
                </div>
              </div>
            )}

            {builtinInfo.length === 0 && sortingColumns.length === 0 && (
              <div className="flex flex-col items-center justify-center py-12 text-muted-foreground gap-2">
                <BarChart3 className="h-8 w-8 opacity-30" />
                <p className="text-sm">No built-in optimizations detected</p>
              </div>
            )}
          </div>
        </ScrollArea>
      </TabsContent>

      {/* Custom Indices */}
      <TabsContent value="custom" className="flex-1 min-h-0">
        <ScrollArea className="h-full">
          <div className="p-4 space-y-4">
            {hasCustom ? (
              <>
                <div className="flex items-center justify-between">
                  <p className="text-xs text-muted-foreground">
                    Footer-embedded indices written by Parquet.Query indexing extensions.
                  </p>
                  <Badge variant="outline" className="text-[10px]">
                    {customIndices.length} {customIndices.length === 1 ? 'index' : 'indices'} on {byColumn.size} {byColumn.size === 1 ? 'column' : 'columns'}
                  </Badge>
                </div>

                {[...byColumn.entries()].map(([columnPath, columnIndices]) => (
                  <div key={columnPath} className="rounded-lg border bg-card">
                    <div className="px-4 py-2.5 border-b bg-muted/30">
                      <span className="text-sm font-mono font-medium">{columnPath}</span>
                    </div>
                    <div className="divide-y">
                      {columnIndices.map((idx) => (
                        <IndexCard
                          key={`${idx.columnPath}-${idx.indexType}`}
                          idx={idx}
                          maxRowGroup={maxRowGroup}
                        />
                      ))}
                    </div>
                  </div>
                ))}
              </>
            ) : (
              <div className="flex flex-col items-center justify-center py-12 text-muted-foreground gap-2">
                <Hash className="h-8 w-8 opacity-30" />
                <p className="text-sm">No custom indices found in this file</p>
                <p className="text-xs">Use Parquet.Query indexing extensions to add bitmap or Lucene indices.</p>
              </div>
            )}
          </div>
        </ScrollArea>
      </TabsContent>
    </Tabs>
  )
}
