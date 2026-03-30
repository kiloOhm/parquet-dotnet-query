import { useCallback, useState } from 'react'
import { bridge } from '@/api/bridge'
import type { DataPage, ParquetFileInfo, QueryPlan, QueryPredicate } from '@/api/types'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { formatMs, formatNumber } from '@/lib/utils'
import {
  Play,
  Plus,
  Search,
  Trash2,
  X,
  Loader2,
  CheckCircle2,
  XCircle,
} from 'lucide-react'

const OPERATORS = [
  { value: 'eq', label: '=' },
  { value: 'neq', label: '!=' },
  { value: 'gt', label: '>' },
  { value: 'ge', label: '>=' },
  { value: 'lt', label: '<' },
  { value: 'le', label: '<=' },
  { value: 'between', label: 'BETWEEN' },
  { value: 'startsWith', label: 'STARTS WITH' },
]

interface QueryEditorProps {
  fileInfo: ParquetFileInfo | null
}

interface PredicateRow {
  id: number
  column: string
  operator: string
  value: string
  value2: string
}

let nextId = 1

export function QueryEditor({ fileInfo }: QueryEditorProps) {
  const [predicates, setPredicates] = useState<PredicateRow[]>([])
  const [plan, setPlan] = useState<QueryPlan | null>(null)
  const [resultData, setResultData] = useState<DataPage | null>(null)
  const [loading, setLoading] = useState(false)
  const [planLoading, setPlanLoading] = useState(false)

  const addPredicate = () => {
    const firstCol = fileInfo?.schema.columns[0]?.name ?? ''
    setPredicates((prev) => [
      ...prev,
      { id: nextId++, column: firstCol, operator: 'eq', value: '', value2: '' },
    ])
  }

  const removePredicate = (id: number) => {
    setPredicates((prev) => prev.filter((p) => p.id !== id))
  }

  const updatePredicate = (id: number, field: keyof PredicateRow, value: string) => {
    setPredicates((prev) =>
      prev.map((p) => (p.id === id ? { ...p, [field]: value } : p)),
    )
  }

  const buildPredicates = (): QueryPredicate[] =>
    predicates
      .filter((p) => p.column && p.value)
      .map((p) => ({
        column: p.column,
        operator: p.operator,
        value: p.value,
        ...(p.operator === 'between' && p.value2 ? { value2: p.value2 } : {}),
      }))

  const explainPlan = useCallback(async () => {
    if (!fileInfo) return
    setPlanLoading(true)
    try {
      const queryPlan = await bridge.getQueryPlan(buildPredicates())
      setPlan(queryPlan)
      setResultData(null)
    } catch (err) {
      console.error('Failed to get query plan:', err)
    } finally {
      setPlanLoading(false)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fileInfo, predicates])

  const executeQuery = useCallback(async () => {
    if (!fileInfo) return
    setLoading(true)
    try {
      const result = await bridge.executeQuery(buildPredicates(), 0, 200)
      setPlan(result.plan)
      setResultData(result.data)
    } catch (err) {
      console.error('Failed to execute query:', err)
    } finally {
      setLoading(false)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fileInfo, predicates])

  if (!fileInfo) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        Open a file to build queries
      </div>
    )
  }

  const columns = fileInfo.schema.columns

  return (
    <div className="flex h-full">
      {/* Left: Predicate builder */}
      <div className="w-[420px] border-r flex flex-col">
        <div className="p-3 border-b bg-muted/30">
          <h3 className="text-sm font-semibold">Predicate Pushdown Filter</h3>
          <p className="text-xs text-muted-foreground mt-0.5">
            Build predicates to test row group pruning
          </p>
        </div>

        <ScrollArea className="flex-1">
          <div className="p-3 space-y-3">
            {predicates.map((pred) => (
              <div key={pred.id} className="rounded-md border p-3 space-y-2 bg-card">
                <div className="flex items-center justify-between">
                  <Label className="text-xs">Column</Label>
                  <Button
                    variant="ghost" size="icon" className="h-6 w-6"
                    onClick={() => removePredicate(pred.id)}
                  >
                    <X className="h-3 w-3" />
                  </Button>
                </div>
                <Select
                  value={pred.column}
                  onValueChange={(v) => updatePredicate(pred.id, 'column', v)}
                >
                  <SelectTrigger className="h-8 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {columns.map((col) => (
                      <SelectItem key={col.name} value={col.name} className="text-xs">
                        {col.name}
                        <span className="text-muted-foreground ml-2">{col.dataType}</span>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>

                <div className="flex gap-2">
                  <div className="flex-1">
                    <Label className="text-xs">Operator</Label>
                    <Select
                      value={pred.operator}
                      onValueChange={(v) => updatePredicate(pred.id, 'operator', v)}
                    >
                      <SelectTrigger className="h-8 text-xs mt-1">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {OPERATORS.map((op) => (
                          <SelectItem key={op.value} value={op.value} className="text-xs">
                            {op.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="flex-1">
                    <Label className="text-xs">Value</Label>
                    <Input
                      className="h-8 text-xs mt-1"
                      value={pred.value}
                      onChange={(e) => updatePredicate(pred.id, 'value', e.target.value)}
                      placeholder="value..."
                    />
                  </div>
                </div>

                {pred.operator === 'between' && (
                  <div>
                    <Label className="text-xs">Upper Bound</Label>
                    <Input
                      className="h-8 text-xs mt-1"
                      value={pred.value2}
                      onChange={(e) => updatePredicate(pred.id, 'value2', e.target.value)}
                      placeholder="upper bound..."
                    />
                  </div>
                )}
              </div>
            ))}

            <Button
              variant="outline" size="sm" className="w-full"
              onClick={addPredicate}
            >
              <Plus className="h-3 w-3 mr-1" /> Add Predicate
            </Button>
          </div>
        </ScrollArea>

        <div className="p-3 border-t flex gap-2">
          <Button
            variant="secondary" size="sm" className="flex-1"
            disabled={planLoading || loading}
            onClick={() => void explainPlan()}
          >
            {planLoading ? (
              <Loader2 className="h-3 w-3 mr-1 animate-spin" />
            ) : (
              <Search className="h-3 w-3 mr-1" />
            )}
            Explain
          </Button>
          <Button
            size="sm" className="flex-1"
            disabled={loading || planLoading}
            onClick={() => void executeQuery()}
          >
            {loading ? (
              <Loader2 className="h-3 w-3 mr-1 animate-spin" />
            ) : (
              <Play className="h-3 w-3 mr-1" />
            )}
            Execute
          </Button>
          {predicates.length > 0 && (
            <Button
              variant="ghost" size="icon" className="h-9 w-9"
              onClick={() => {
                setPredicates([])
                setPlan(null)
                setResultData(null)
              }}
            >
              <Trash2 className="h-3 w-3" />
            </Button>
          )}
        </div>
      </div>

      {/* Right: Results */}
      <div className="flex-1 flex flex-col">
        {/* Plan visualization */}
        {plan && (
          <div className="border-b">
            <div className="p-3 bg-muted/30">
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-semibold">Query Plan</h3>
                <div className="flex items-center gap-3 text-xs text-muted-foreground">
                  <span>
                    {plan.selectedRowGroups}/{plan.totalRowGroups} row groups
                  </span>
                  <span>{formatNumber(plan.candidateRows)} candidate rows</span>
                  {plan.matchedRows >= 0 && (
                    <span>{formatNumber(plan.matchedRows)} matched</span>
                  )}
                  <Badge variant="outline" className="text-[10px]">
                    {formatMs(plan.executionMs)}
                  </Badge>
                </div>
              </div>
            </div>
            <ScrollArea className="max-h-[200px]">
              <div className="p-2 space-y-1">
                {plan.decisions.map((d) => (
                  <div
                    key={d.index}
                    className={`flex items-start gap-2 px-3 py-1.5 rounded text-xs ${
                      d.shouldRead ? 'rg-read' : 'rg-skip'
                    }`}
                  >
                    {d.shouldRead ? (
                      <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500 mt-0.5 shrink-0" />
                    ) : (
                      <XCircle className="h-3.5 w-3.5 text-red-500 mt-0.5 shrink-0" />
                    )}
                    <div className="min-w-0">
                      <span className="font-medium">
                        RG {d.index}
                      </span>
                      <span className="text-muted-foreground ml-1.5">
                        ({formatNumber(d.rowCount)} rows)
                      </span>
                      <span className="text-muted-foreground ml-1.5">
                        {d.shouldRead ? 'READ' : 'SKIP'}
                      </span>
                      <p className="text-muted-foreground mt-0.5 break-all">{d.reason}</p>
                    </div>
                  </div>
                ))}
              </div>
            </ScrollArea>
          </div>
        )}

        {/* Result data table */}
        <div className="flex-1 overflow-auto">
          {resultData ? (
            <div>
              <div className="px-3 py-2 border-b bg-muted/30 text-xs text-muted-foreground">
                Showing {formatNumber(resultData.rows.length)} of{' '}
                {formatNumber(resultData.totalRows)} matched rows
              </div>
              <table className="w-full border-collapse text-sm">
                <thead className="sticky top-0 z-10">
                  <tr>
                    {resultData.columns.map((col) => (
                      <th
                        key={col}
                        className="bg-muted px-3 py-1.5 text-left text-xs font-medium text-muted-foreground border-b border-r"
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {resultData.rows.map((row, ri) => (
                    <tr key={ri} className="hover:bg-accent/30">
                      {row.map((cell, ci) => (
                        <td key={ci} className="px-3 py-1 border-b border-r text-xs truncate max-w-[200px]">
                          {cell === null || cell === undefined ? (
                            <span className="text-muted-foreground/50 italic">null</span>
                          ) : (
                            String(cell)
                          )}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : plan ? (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
              Click Execute to see matching rows
            </div>
          ) : (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
              Add predicates and click Explain or Execute
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
