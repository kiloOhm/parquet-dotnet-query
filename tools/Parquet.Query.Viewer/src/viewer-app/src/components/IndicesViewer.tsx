import { useEffect, useMemo, useState } from 'react'
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

function EntryBrowser({ entries, indexType, maxRowGroup }: {
  entries: IndexEntry[]
  indexType: string
  maxRowGroup: number
}) {
  const [filter, setFilter] = useState('')
  const filtered = useMemo(() => {
    if (!filter) return entries
    const lower = filter.toLowerCase()
    return entries.filter(e => e.key.toLowerCase().includes(lower))
  }, [entries, filter])

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
            placeholder={`Filter ${entries.length} ${entries.length === 1 ? 'entry' : 'entries'}...`}
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
        </div>
        <span className="text-[10px] text-muted-foreground whitespace-nowrap tabular-nums">
          {filtered.length === entries.length
            ? `${entries.length} entries`
            : `${filtered.length} / ${entries.length}`}
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
        <ScrollArea className="max-h-[280px]">
          <table className="w-full text-xs">
            <tbody>
              {filtered.length === 0 ? (
                <tr>
                  <td colSpan={2} className="px-3 py-4 text-center text-muted-foreground">
                    {filter ? 'No matching entries' : 'No entries'}
                  </td>
                </tr>
              ) : (
                filtered.map((entry) => (
                  <tr key={entry.key} className="border-b last:border-0 hover:bg-muted/20">
                    <td className="px-3 py-1.5 font-mono truncate max-w-[200px]" title={entry.key}>
                      {entry.key}
                    </td>
                    <td className="px-3 py-1.5">
                      <RowGroupDots rowGroups={entry.rowGroups} maxRowGroup={maxRowGroup} />
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </ScrollArea>
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
  const hasEntries = stats?.entries && stats.entries.length > 0

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
              entries={stats!.entries!}
              indexType={idx.indexType}
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
