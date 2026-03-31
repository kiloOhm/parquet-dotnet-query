import { useEffect, useState } from 'react'
import { bridge } from '@/api/bridge'
import type { IndicesInfo, ParquetFileInfo } from '@/api/types'
import { useError } from '@/components/ErrorDialog'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  ArrowDown,
  ArrowUp,
  BarChart3,
  Check,
  Database,
  Fingerprint,
  Hash,
  Minus,
  Search,
} from 'lucide-react'

interface IndicesViewerProps {
  fileInfo: ParquetFileInfo | null
}

const INDEX_ICONS: Record<string, typeof Database> = {
  Bitmap: Database,
  Hash: Fingerprint,
  Lucene: Search,
}

const INDEX_COLORS: Record<string, string> = {
  Bitmap: 'bg-blue-500/10 text-blue-500 border-blue-500/20',
  Hash: 'bg-amber-500/10 text-amber-500 border-amber-500/20',
  Lucene: 'bg-emerald-500/10 text-emerald-500 border-emerald-500/20',
}

function BoolCell({ value }: { value: boolean }) {
  return value ? (
    <Check className="h-4 w-4 text-emerald-500 mx-auto" />
  ) : (
    <Minus className="h-4 w-4 text-muted-foreground/30 mx-auto" />
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
      .catch((err) => {
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
  const hasBuiltin = builtinInfo.length > 0

  if (!hasCustom && !hasBuiltin) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-muted-foreground gap-2">
        <Hash className="h-8 w-8 opacity-30" />
        <p className="text-sm">No indices found in this file</p>
      </div>
    )
  }

  // Group custom indices by column
  const byColumn = new Map<string, typeof customIndices>()
  for (const idx of customIndices) {
    const list = byColumn.get(idx.columnPath) ?? []
    list.push(idx)
    byColumn.set(idx.columnPath, list)
  }

  return (
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
                const Icon = isDesc ? ArrowDown : ArrowUp
                return (
                  <Badge key={col} variant="outline" className="text-xs font-mono gap-1">
                    <Icon className="h-3 w-3" />
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

        {/* Custom indices */}
        {hasCustom && (
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-semibold">Custom Indices</h3>
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
                  {columnIndices.map((idx) => {
                    const Icon = INDEX_ICONS[idx.indexType] ?? Hash
                    const colorClass = INDEX_COLORS[idx.indexType] ?? 'bg-muted text-foreground border-border'
                    return (
                      <div key={`${idx.columnPath}-${idx.indexType}`} className="px-4 py-3 space-y-2">
                        <div className="flex items-center gap-2">
                          <div className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-md text-xs font-medium border ${colorClass}`}>
                            <Icon className="h-3.5 w-3.5" />
                            {idx.indexType}
                          </div>
                        </div>
                        <p className="text-xs text-muted-foreground leading-relaxed">
                          {idx.description}
                        </p>
                        <div className="flex flex-wrap gap-1.5">
                          {idx.acceleratedOperations.map((op) => (
                            <Badge key={op} variant="secondary" className="text-[10px] font-mono">
                              {op}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Built-in optimizations table */}
        {hasBuiltin && (
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-semibold flex items-center gap-2">
                <BarChart3 className="h-4 w-4" />
                Built-in Optimizations
              </h3>
            </div>
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
      </div>
    </ScrollArea>
  )
}
