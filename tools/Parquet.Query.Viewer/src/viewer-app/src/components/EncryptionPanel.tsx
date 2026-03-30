import { useState } from 'react'
import type { EncryptionConfig } from '@/api/types'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Separator } from '@/components/ui/separator'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Lock, Plus, Trash2, X } from 'lucide-react'

interface EncryptionPanelProps {
  onApply: (config: EncryptionConfig | undefined) => void
  onClose: () => void
}

interface ColumnKeyEntry {
  id: number
  column: string
  key: string
}

let nextEntryId = 1

export function EncryptionPanel({ onApply, onClose }: EncryptionPanelProps) {
  const [footerKey, setFooterKey] = useState('')
  const [footerSigningKey, setFooterSigningKey] = useState('')
  const [plaintextFooter, setPlaintextFooter] = useState(false)
  const [useCtr, setUseCtr] = useState(false)
  const [aadPrefix, setAadPrefix] = useState('')
  const [columnKeys, setColumnKeys] = useState<ColumnKeyEntry[]>([])

  const addColumnKey = () => {
    setColumnKeys((prev) => [...prev, { id: nextEntryId++, column: '', key: '' }])
  }

  const removeColumnKey = (id: number) => {
    setColumnKeys((prev) => prev.filter((e) => e.id !== id))
  }

  const updateColumnKey = (id: number, field: 'column' | 'key', value: string) => {
    setColumnKeys((prev) => prev.map((e) => (e.id === id ? { ...e, [field]: value } : e)))
  }

  const handleApply = () => {
    const hasAny = footerKey || footerSigningKey || columnKeys.length > 0

    if (!hasAny) {
      onApply(undefined)
      return
    }

    const colKeys: Record<string, string> = {}
    for (const entry of columnKeys) {
      if (entry.column && entry.key) {
        colKeys[entry.column] = entry.key
      }
    }

    onApply({
      footerKey: footerKey || undefined,
      footerSigningKey: footerSigningKey || undefined,
      plaintextFooter,
      useCtr,
      aadPrefix: aadPrefix || undefined,
      columnKeys: Object.keys(colKeys).length > 0 ? colKeys : undefined,
    })
  }

  const handleClear = () => {
    setFooterKey('')
    setFooterSigningKey('')
    setPlaintextFooter(false)
    setUseCtr(false)
    setAadPrefix('')
    setColumnKeys([])
    onApply(undefined)
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between p-3 border-b bg-muted/30">
        <h3 className="flex items-center gap-2 text-sm font-semibold">
          <Lock className="h-4 w-4" /> Modular Encryption
        </h3>
        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onClose}>
          <X className="h-4 w-4" />
        </Button>
      </div>

      <ScrollArea className="flex-1">
        <div className="p-4 space-y-5">
          {/* Footer Key */}
          <div className="space-y-2">
            <Label className="text-xs font-medium">Footer Encryption Key (hex)</Label>
            <Input
              className="h-8 text-xs font-mono"
              value={footerKey}
              onChange={(e) => setFooterKey(e.target.value)}
              placeholder="e.g. 0123456789abcdef0123456789abcdef"
            />
            <p className="text-[10px] text-muted-foreground">
              AES-128 (32 hex chars), AES-192 (48 hex), or AES-256 (64 hex)
            </p>
          </div>

          {/* Footer Signing Key */}
          <div className="space-y-2">
            <Label className="text-xs font-medium">Footer Signing Key (hex)</Label>
            <Input
              className="h-8 text-xs font-mono"
              value={footerSigningKey}
              onChange={(e) => setFooterSigningKey(e.target.value)}
              placeholder="signing key for plaintext footer verification"
            />
          </div>

          <Separator />

          {/* Toggles */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <div>
                <Label className="text-xs font-medium">Plaintext Footer</Label>
                <p className="text-[10px] text-muted-foreground">
                  Footer is not encrypted (readable without key)
                </p>
              </div>
              <Switch checked={plaintextFooter} onCheckedChange={setPlaintextFooter} />
            </div>
            <div className="flex items-center justify-between">
              <div>
                <Label className="text-xs font-medium">CTR Variant</Label>
                <p className="text-[10px] text-muted-foreground">
                  Use AES_GCM_CTR_V1 instead of AES_GCM_V1
                </p>
              </div>
              <Switch checked={useCtr} onCheckedChange={setUseCtr} />
            </div>
          </div>

          <Separator />

          {/* AAD Prefix */}
          <div className="space-y-2">
            <Label className="text-xs font-medium">AAD Prefix</Label>
            <Input
              className="h-8 text-xs font-mono"
              value={aadPrefix}
              onChange={(e) => setAadPrefix(e.target.value)}
              placeholder="additional authenticated data prefix"
            />
          </div>

          <Separator />

          {/* Column Keys */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <Label className="text-xs font-medium">Column Encryption Keys</Label>
              <Button variant="outline" size="sm" className="h-7 text-xs" onClick={addColumnKey}>
                <Plus className="h-3 w-3 mr-1" /> Add
              </Button>
            </div>
            {columnKeys.map((entry) => (
              <div key={entry.id} className="flex gap-2 items-start">
                <div className="flex-1 space-y-1">
                  <Input
                    className="h-7 text-xs font-mono"
                    value={entry.column}
                    onChange={(e) => updateColumnKey(entry.id, 'column', e.target.value)}
                    placeholder="column path"
                  />
                  <Input
                    className="h-7 text-xs font-mono"
                    value={entry.key}
                    onChange={(e) => updateColumnKey(entry.id, 'key', e.target.value)}
                    placeholder="hex key"
                  />
                </div>
                <Button
                  variant="ghost" size="icon" className="h-7 w-7 mt-0.5"
                  onClick={() => removeColumnKey(entry.id)}
                >
                  <Trash2 className="h-3 w-3" />
                </Button>
              </div>
            ))}
            {columnKeys.length === 0 && (
              <p className="text-[10px] text-muted-foreground">
                No per-column keys configured. All columns use the footer key.
              </p>
            )}
          </div>
        </div>
      </ScrollArea>

      <div className="p-3 border-t flex gap-2">
        <Button size="sm" className="flex-1" onClick={handleApply}>
          Apply & Reopen
        </Button>
        <Button variant="outline" size="sm" onClick={handleClear}>
          Clear
        </Button>
      </div>
    </div>
  )
}
