import { useCallback, useEffect, useState } from 'react'
import { bridge } from '@/api/bridge'
import type { FileMetadataInfo, ParquetFileInfo } from '@/api/types'
import { useError } from '@/components/ErrorDialog'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Separator } from '@/components/ui/separator'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { formatBytes, formatNumber } from '@/lib/utils'
import {
  ChevronRight,
  Database,
  FileText,
  Layers,
  Lock,
  Table2,
} from 'lucide-react'

interface MetadataViewerProps {
  fileInfo: ParquetFileInfo | null
}

export function MetadataViewer({ fileInfo }: MetadataViewerProps) {
  const [metadata, setMetadata] = useState<FileMetadataInfo | null>(null)
  const [expandedRgs, setExpandedRgs] = useState<Set<number>>(new Set())
  const showError = useError()

  const fetchMetadata = useCallback(async () => {
    if (!fileInfo) return
    try {
      const md = await bridge.getMetadata()
      setMetadata(md)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      showError('Failed to fetch metadata', msg)
    }
  }, [fileInfo])

  useEffect(() => {
    if (fileInfo) {
      void fetchMetadata()
    } else {
      setMetadata(null)
    }
  }, [fileInfo, fetchMetadata])

  const toggleRg = (idx: number) => {
    setExpandedRgs((prev) => {
      const next = new Set(prev)
      if (next.has(idx)) next.delete(idx)
      else next.add(idx)
      return next
    })
  }

  if (!fileInfo) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        Open a file to view metadata
      </div>
    )
  }

  return (
    <ScrollArea className="h-full">
      <div className="p-4 space-y-6">
        {/* File Info */}
        <section>
          <h3 className="flex items-center gap-2 text-sm font-semibold mb-3">
            <FileText className="h-4 w-4" /> File Information
          </h3>
          <div className="grid grid-cols-2 gap-x-8 gap-y-1.5 text-sm">
            <InfoRow label="Path" value={fileInfo.path} />
            <InfoRow label="Size" value={formatBytes(fileInfo.fileSize)} />
            <InfoRow label="Format" value={fileInfo.fileFormat} />
            <InfoRow label="Row Groups" value={String(fileInfo.rowGroupCount)} />
            <InfoRow label="Total Rows" value={formatNumber(fileInfo.totalRowCount)} />
            <InfoRow
              label="Encrypted"
              value={
                fileInfo.isEncrypted ? (
                  <Badge variant="destructive" className="text-[10px] px-1.5 py-0">Yes</Badge>
                ) : (
                  <Badge variant="secondary" className="text-[10px] px-1.5 py-0">No</Badge>
                )
              }
            />
          </div>
        </section>

        <Separator />

        {/* Schema */}
        <section>
          <h3 className="flex items-center gap-2 text-sm font-semibold mb-3">
            <Table2 className="h-4 w-4" /> Schema ({fileInfo.schema.columns.length} columns)
          </h3>
          <div className="rounded-md border overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-muted/50">
                  <th className="text-left px-3 py-1.5 font-medium">Name</th>
                  <th className="text-left px-3 py-1.5 font-medium">Type</th>
                  <th className="text-left px-3 py-1.5 font-medium">CLR Type</th>
                  <th className="text-center px-3 py-1.5 font-medium">Nullable</th>
                  <th className="text-center px-3 py-1.5 font-medium">Repeated</th>
                </tr>
              </thead>
              <tbody>
                {fileInfo.schema.columns.map((col) => (
                  <tr key={col.path} className="border-t">
                    <td className="px-3 py-1.5 font-mono text-xs">{col.name}</td>
                    <td className="px-3 py-1.5">
                      <Badge variant="outline" className="text-[10px] font-mono">{col.dataType}</Badge>
                    </td>
                    <td className="px-3 py-1.5 text-muted-foreground text-xs">{col.clrType}</td>
                    <td className="px-3 py-1.5 text-center">{col.isNullable ? 'Yes' : '-'}</td>
                    <td className="px-3 py-1.5 text-center">{col.isRepeated ? 'Yes' : '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <Separator />

        {/* Footer */}
        {metadata?.footer && (
          <section>
            <h3 className="flex items-center gap-2 text-sm font-semibold mb-3">
              <Database className="h-4 w-4" /> Footer
            </h3>
            <div className="grid grid-cols-2 gap-x-8 gap-y-1.5 text-sm mb-3">
              <InfoRow label="Created By" value={metadata.footer.createdBy} />
              <InfoRow label="Version" value={String(metadata.footer.version)} />
            </div>
            {Object.keys(metadata.footer.keyValueMetadata).length > 0 && (
              <div>
                <p className="text-xs font-medium text-muted-foreground mb-1.5">Key-Value Metadata</p>
                <div className="rounded-md border overflow-hidden">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-muted/50">
                        <th className="text-left px-3 py-1.5 font-medium">Key</th>
                        <th className="text-left px-3 py-1.5 font-medium">Value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(metadata.footer.keyValueMetadata).map(([k, v]) => (
                        <tr key={k} className="border-t">
                          <td className="px-3 py-1.5 font-mono text-xs">{k}</td>
                          <td className="px-3 py-1.5 text-xs break-all">{v}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </section>
        )}

        <Separator />

        {/* Encryption */}
        {metadata?.encryption && (
          <>
            <section>
              <h3 className="flex items-center gap-2 text-sm font-semibold mb-3">
                <Lock className="h-4 w-4" /> Encryption
              </h3>
              <div className="grid grid-cols-2 gap-x-8 gap-y-1.5 text-sm">
                <InfoRow label="Algorithm" value={metadata.encryption.algorithm} />
                <InfoRow
                  label="Footer Encrypted"
                  value={metadata.encryption.hasEncryptedFooter ? 'Yes' : 'No (plaintext)'}
                />
                {metadata.encryption.encryptedColumns.length > 0 && (
                  <InfoRow
                    label="Encrypted Columns"
                    value={metadata.encryption.encryptedColumns.join(', ')}
                  />
                )}
              </div>
            </section>
            <Separator />
          </>
        )}

        {/* Row Groups */}
        {metadata?.rowGroups && (
          <section>
            <h3 className="flex items-center gap-2 text-sm font-semibold mb-3">
              <Layers className="h-4 w-4" /> Row Groups ({metadata.rowGroups.length})
            </h3>
            <div className="space-y-1.5">
              {metadata.rowGroups.map((rg) => (
                <Collapsible
                  key={rg.index}
                  open={expandedRgs.has(rg.index)}
                  onOpenChange={() => toggleRg(rg.index)}
                >
                  <CollapsibleTrigger className="flex items-center w-full gap-2 px-3 py-2 text-sm rounded-md hover:bg-accent/50 transition-colors">
                    <ChevronRight
                      className={`h-4 w-4 transition-transform ${expandedRgs.has(rg.index) ? 'rotate-90' : ''}`}
                    />
                    <span className="font-medium">Row Group {rg.index}</span>
                    <Badge variant="secondary" className="text-[10px]">
                      {formatNumber(rg.rowCount)} rows
                    </Badge>
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <div className="ml-6 mt-1 rounded-md border overflow-hidden">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="bg-muted/50">
                            <th className="text-left px-2 py-1 font-medium">Column</th>
                            <th className="text-left px-2 py-1 font-medium">Type</th>
                            <th className="text-left px-2 py-1 font-medium">Min</th>
                            <th className="text-left px-2 py-1 font-medium">Max</th>
                            <th className="text-right px-2 py-1 font-medium">Nulls</th>
                            <th className="text-left px-2 py-1 font-medium">Codec</th>
                            <th className="text-right px-2 py-1 font-medium">Compressed</th>
                            <th className="text-right px-2 py-1 font-medium">Uncompressed</th>
                          </tr>
                        </thead>
                        <tbody>
                          {rg.columns.map((col) => (
                            <tr key={col.columnName} className="border-t">
                              <td className="px-2 py-1 font-mono">{col.columnName}</td>
                              <td className="px-2 py-1">{col.dataType}</td>
                              <td className="px-2 py-1 text-muted-foreground">
                                {col.minValue != null ? String(col.minValue) : '-'}
                              </td>
                              <td className="px-2 py-1 text-muted-foreground">
                                {col.maxValue != null ? String(col.maxValue) : '-'}
                              </td>
                              <td className="px-2 py-1 text-right">{col.nullCount ?? '-'}</td>
                              <td className="px-2 py-1">
                                {col.compression && (
                                  <Badge variant="outline" className="text-[9px] px-1 py-0">{col.compression}</Badge>
                                )}
                              </td>
                              <td className="px-2 py-1 text-right">{formatBytes(col.compressedSize)}</td>
                              <td className="px-2 py-1 text-right">{formatBytes(col.uncompressedSize)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              ))}
            </div>
          </section>
        )}
      </div>
    </ScrollArea>
  )
}

function InfoRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <>
      <span className="text-muted-foreground">{label}</span>
      <span className="font-mono text-xs break-all">{value}</span>
    </>
  )
}
