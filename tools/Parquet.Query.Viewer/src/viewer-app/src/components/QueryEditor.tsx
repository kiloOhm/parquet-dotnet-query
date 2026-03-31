import { useCallback, useState } from 'react'
import { bridge } from '@/api/bridge'
import type { DataPage, ParquetFileInfo, QueryPlan, QueryPredicate } from '@/api/types'
import { useError } from '@/components/ErrorDialog'
import { DataGrid } from '@/components/DataGrid'
import { buildCodeSnippet } from '@/components/query-language'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { formatMs, formatNumber } from '@/lib/utils'
import {
  CheckCircle2,
  Code2,
  Loader2,
  Play,
  Plus,
  Sparkles,
  Trash2,
  X,
  XCircle,
} from 'lucide-react'

const OPERATORS = [
  { value: '==', label: '==', group: 'Comparison' },
  { value: '!=', label: '!=', group: 'Comparison' },
  { value: '>', label: '>', group: 'Comparison' },
  { value: '>=', label: '>=', group: 'Comparison' },
  { value: '<', label: '<', group: 'Comparison' },
  { value: '<=', label: '<=', group: 'Comparison' },
  { value: 'Between', label: 'Between', group: 'Range' },
  { value: 'IsNull', label: 'IsNull', group: 'Null' },
  { value: 'IsNotNull', label: 'IsNotNull', group: 'Null' },
  { value: 'StartsWith', label: 'StartsWith', group: 'String' },
  { value: 'EndsWith', label: 'EndsWith', group: 'String' },
  { value: 'Contains', label: 'Contains', group: 'String' },
  { value: 'LuceneMatch', label: 'LuceneMatch', group: 'Lucene' },
  { value: 'LuceneFuzzy', label: 'LuceneFuzzy', group: 'Lucene' },
]

/** Operators that don't need a value input */
const NO_VALUE_OPERATORS = new Set(['IsNull', 'IsNotNull'])

interface QueryEditorProps {
  fileInfo: ParquetFileInfo | null
}

interface PredicateRow {
  id: number
  column: string
  operator: string
  value: string
  value2: string
  maxEdits: string
  prefixLength: string
  transpositions: boolean
}

let nextId = 1

export function QueryEditor({ fileInfo }: QueryEditorProps) {
  const [predicates, setPredicates] = useState<PredicateRow[]>([])
  const [plan, setPlan] = useState<QueryPlan | null>(null)
  const [resultData, setResultData] = useState<DataPage | null>(null)
  const [loading, setLoading] = useState(false)
  const [activeTab, setActiveTab] = useState('results')
  const showError = useError()

  const addPredicate = () => {
    const firstCol = fileInfo?.schema.columns[0]?.name ?? ''
    setPredicates((prev) => [
      ...prev,
      { id: nextId++, column: firstCol, operator: '==', value: '', value2: '', maxEdits: '1', prefixLength: '0', transpositions: true },
    ])
  }

  const removePredicate = (id: number) => {
    setPredicates((prev) => prev.filter((p) => p.id !== id))
  }

  const updatePredicate = (id: number, field: keyof PredicateRow, value: string | boolean | null) => {
    if (value === null) return
    setPredicates((prev) =>
      prev.map((p) => (p.id === id ? { ...p, [field]: value } : p)),
    )
  }

  const buildPredicates = (): QueryPredicate[] =>
    predicates
      .filter((p) => p.column && (NO_VALUE_OPERATORS.has(p.operator) || p.value))
      .map((p) => ({
        column: p.column,
        operator: p.operator,
        value: NO_VALUE_OPERATORS.has(p.operator) ? '' : p.value,
        ...(p.operator === 'Between' && p.value2 ? { value2: p.value2 } : {}),
        ...(p.operator === 'LuceneFuzzy' ? {
          maxEdits: parseInt(p.maxEdits) || 1,
          prefixLength: parseInt(p.prefixLength) || 0,
          transpositions: p.transpositions,
        } : {}),
      }))

  const executeQuery = useCallback(async () => {
    if (!fileInfo) return
    setLoading(true)
    setActiveTab('results')
    try {
      const result = await bridge.executeQuery(buildPredicates(), 0, 200)
      setPlan(result.plan)
      setResultData(result.data)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      showError('Failed to execute query', msg)
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
  const builtPredicates = buildPredicates()
  const codeSnippet = buildCodeSnippet(fileInfo, builtPredicates)
  const canRunQuery = builtPredicates.length > 0

  return (
    <div className="flex h-full">
      {/* Left: Predicate builder */}
      <div className="w-[400px] border-r flex flex-col min-h-0">
        <div className="px-3 py-2 border-b bg-muted/30 flex items-center justify-between shrink-0">
          <div>
            <h3 className="text-sm font-semibold">Predicates</h3>
            <p className="text-[10px] text-muted-foreground">Row group pushdown filter</p>
          </div>
          <Button variant="outline" size="sm" className="h-7 text-xs" onClick={addPredicate}>
            <Plus className="h-3 w-3 mr-1" /> Add
          </Button>
        </div>

        <ScrollArea className="flex-1 min-h-0">
          <div className="p-2 space-y-1.5">
            {predicates.length === 0 && (
              <div className="text-xs text-muted-foreground text-center py-8">
                Click "Add" to create a predicate
              </div>
            )}
            {predicates.map((pred) => (
              <div key={pred.id} className="rounded border p-2 bg-card space-y-1.5">
                {/* Row 1: Column + remove */}
                <div className="flex items-center gap-1.5">
                  <Select
                    value={pred.column}
                    onValueChange={(v) => updatePredicate(pred.id, 'column', v)}
                  >
                    <SelectTrigger className="h-7 text-xs flex-1">
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
                  <Button
                    variant="ghost" size="icon" className="h-7 w-7 shrink-0"
                    onClick={() => removePredicate(pred.id)}
                  >
                    <X className="h-3 w-3" />
                  </Button>
                </div>

                {/* Row 2: Operator + Value */}
                <div className="flex items-center gap-1.5">
                  <Select
                    value={pred.operator}
                    onValueChange={(v) => updatePredicate(pred.id, 'operator', v)}
                  >
                    <SelectTrigger className="h-7 text-xs w-[130px] shrink-0 font-mono">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {OPERATORS.map((op) => (
                        <SelectItem key={op.value} value={op.value} className="text-xs font-mono">
                          {op.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  {!NO_VALUE_OPERATORS.has(pred.operator) && (
                    <Input
                      className="h-7 text-xs flex-1"
                      value={pred.value}
                      onChange={(e) => updatePredicate(pred.id, 'value', e.target.value)}
                      placeholder="value..."
                    />
                  )}
                </div>

                {/* Between: upper bound */}
                {pred.operator === 'Between' && (
                  <div className="flex items-center gap-1.5">
                    <span className="text-[10px] text-muted-foreground w-[130px] shrink-0 text-right pr-1">to</span>
                    <Input
                      className="h-7 text-xs flex-1"
                      value={pred.value2}
                      onChange={(e) => updatePredicate(pred.id, 'value2', e.target.value)}
                      placeholder="upper bound..."
                    />
                  </div>
                )}

                {/* LuceneFuzzy: options */}
                {pred.operator === 'LuceneFuzzy' && (
                  <div className="flex items-center gap-1.5 pt-0.5 border-t">
                    <div className="flex items-center gap-1">
                      <Label className="text-[10px] text-muted-foreground whitespace-nowrap">edits</Label>
                      <Input className="h-7 text-xs w-12" type="number" min={0} max={2} value={pred.maxEdits} onChange={(e) => updatePredicate(pred.id, 'maxEdits', e.target.value)} />
                    </div>
                    <div className="flex items-center gap-1">
                      <Label className="text-[10px] text-muted-foreground whitespace-nowrap">prefix</Label>
                      <Input className="h-7 text-xs w-12" type="number" min={0} value={pred.prefixLength} onChange={(e) => updatePredicate(pred.id, 'prefixLength', e.target.value)} />
                    </div>
                    <div className="flex items-center gap-1 ml-auto">
                      <input type="checkbox" checked={pred.transpositions} onChange={(e) => updatePredicate(pred.id, 'transpositions', e.target.checked)} className="h-3 w-3" />
                      <Label className="text-[10px] text-muted-foreground">transpose</Label>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </ScrollArea>

        <div className="px-2 py-2 border-t flex gap-2 shrink-0">
          <Button
            size="sm" className="flex-1 h-8"
            disabled={loading || !canRunQuery}
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
              variant="ghost" size="icon" className="h-8 w-8 shrink-0"
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

      {/* Right: Results / Plan / Code */}
      <div className="flex-1 min-w-0 flex flex-col">
        <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 gap-0 overflow-hidden">
          <div className="p-3 border-b bg-muted/30 flex items-center justify-between gap-3 shrink-0">
            <TabsList variant="line">
              <TabsTrigger value="results" className="text-xs">Results</TabsTrigger>
              <TabsTrigger value="plan" className="text-xs">Plan</TabsTrigger>
              <TabsTrigger value="code" className="text-xs gap-1.5">
                <Code2 className="h-3.5 w-3.5" /> Generated C#
              </TabsTrigger>
            </TabsList>
            <div className="flex items-center gap-3 text-xs text-muted-foreground">
              {resultData ? (
                <>
                  <span>{formatNumber(resultData.totalRows)} matched rows</span>
                  <span>{formatNumber(resultData.rows.length)} loaded</span>
                </>
              ) : plan ? (
                <>
                  <span>
                    {plan.selectedRowGroups}/{plan.totalRowGroups} row groups
                  </span>
                  <span>{formatNumber(plan.candidateRows)} candidate rows</span>
                  <Badge variant="outline" className="text-[10px]">
                    {formatMs(plan.executionMs)}
                  </Badge>
                </>
              ) : (
                <span>No query run yet</span>
              )}
            </div>
          </div>

          <TabsContent value="results" className="flex-1 overflow-hidden flex flex-col">
            {resultData && (
              <div className="px-3 py-2 border-b bg-muted/30 text-xs text-muted-foreground shrink-0">
                Showing {formatNumber(resultData.rows.length)} of {formatNumber(resultData.totalRows)} matched rows
              </div>
            )}
            <DataGrid
              data={resultData}
              emptyMessage={canRunQuery ? 'Execute the query to load matching rows.' : 'Add predicates and click Execute.'}
            />
          </TabsContent>

          <TabsContent value="plan" className="flex-1 overflow-hidden">
            {plan ? (
              <div className="flex h-full flex-col">
                <div className="px-3 py-2 border-b bg-muted/30 shrink-0">
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
                <ScrollArea className="flex-1">
                  <div className="p-3 space-y-1">
                    {plan.decisions.map((decision) => (
                      <div
                        key={decision.index}
                        className={`flex items-start gap-2 px-3 py-2 rounded text-xs ${
                          decision.shouldRead ? 'rg-read' : 'rg-skip'
                        }`}
                      >
                        {decision.shouldRead ? (
                          <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500 mt-0.5 shrink-0" />
                        ) : (
                          <XCircle className="h-3.5 w-3.5 text-red-500 mt-0.5 shrink-0" />
                        )}
                        <div className="min-w-0">
                          <span className="font-medium">RG {decision.index}</span>
                          <span className="text-muted-foreground ml-1.5">
                            ({formatNumber(decision.rowCount)} rows)
                          </span>
                          <span className="text-muted-foreground ml-1.5">
                            {decision.shouldRead ? 'READ' : 'SKIP'}
                          </span>
                          <p className="text-muted-foreground mt-0.5 break-all">{decision.reason}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </ScrollArea>
              </div>
            ) : (
              <div className="flex h-full items-center justify-center text-muted-foreground text-sm">
                {canRunQuery ? 'Execute the query to inspect row-group pruning.' : 'Add predicates to inspect the plan.'}
              </div>
            )}
          </TabsContent>

          <TabsContent value="code" className="flex-1 overflow-hidden">
            <div className="h-full flex flex-col">
              <div className="px-3 py-2 border-b bg-muted/30 text-xs text-muted-foreground flex items-center gap-2 shrink-0">
                <Sparkles className="h-3.5 w-3.5" />
                Generated Parquet.Query C# code from the current predicates.
              </div>
              <ScrollArea className="flex-1">
                <div className="p-3">
                  <pre className="rounded-md border bg-background p-3 text-[11px] leading-5 overflow-x-auto font-mono">
                    <code>{codeSnippet}</code>
                  </pre>
                </div>
              </ScrollArea>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}
