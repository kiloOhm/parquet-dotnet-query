import { useCallback, useEffect, useState } from 'react'
import { bridge } from '@/api/bridge'
import type { EncryptionConfig, ParquetFileInfo } from '@/api/types'
import { useError } from '@/components/ErrorDialog'
import { DataTable } from '@/components/DataTable'
import { MetadataViewer } from '@/components/MetadataViewer'
import { QueryEditor } from '@/components/QueryEditor'
import { IndicesViewer } from '@/components/IndicesViewer'
import { EncryptionPanel } from '@/components/EncryptionPanel'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { formatBytes, formatNumber } from '@/lib/utils'
import {
  FolderOpen,
  Hash,
  Lock,
  Table2,
  FileText,
  Search,
} from 'lucide-react'

export default function App() {
  const [fileInfo, setFileInfo] = useState<ParquetFileInfo | null>(null)
  const [encryptionConfig, setEncryptionConfig] = useState<EncryptionConfig | undefined>()
  const [showEncryption, setShowEncryption] = useState(false)
  const [activeTab, setActiveTab] = useState('data')
  const showError = useError()
  const [pendingEncryptedPath, setPendingEncryptedPath] = useState<string | null>(null)
  const [openError, setOpenError] = useState<string | null>(null)
  const [dragOver, setDragOver] = useState(false)

  // Listen for files dropped onto the native window
  useEffect(() => {
    return bridge.onPush('fileDropped', (data: { file?: ParquetFileInfo; path?: string; needsEncryption?: boolean; error?: string }) => {
      if (data.needsEncryption && data.path) {
        setPendingEncryptedPath(data.path)
        setShowEncryption(true)
        setFileInfo(null)
        setEncryptionConfig(undefined)
      } else if (data.error && data.path) {
        setPendingEncryptedPath(data.path)
        setOpenError(data.error)
        setShowEncryption(true)
        setFileInfo(null)
        setEncryptionConfig(undefined)
      } else if (data.file) {
        setPendingEncryptedPath(null)
        setFileInfo(data.file)
        setShowEncryption(false)
      }
    })
  }, [])

  // Prevent default browser drag-drop (navigation) and track drag-over state
  useEffect(() => {
    let dragCounter = 0
    const onDragEnter = (e: DragEvent) => { e.preventDefault(); dragCounter++; setDragOver(true) }
    const onDragOver = (e: DragEvent) => { e.preventDefault() }
    const onDragLeave = () => { dragCounter--; if (dragCounter <= 0) { dragCounter = 0; setDragOver(false) } }
    const onDrop = (e: DragEvent) => { e.preventDefault(); dragCounter = 0; setDragOver(false) }

    document.addEventListener('dragenter', onDragEnter)
    document.addEventListener('dragover', onDragOver)
    document.addEventListener('dragleave', onDragLeave)
    document.addEventListener('drop', onDrop)
    return () => {
      document.removeEventListener('dragenter', onDragEnter)
      document.removeEventListener('dragover', onDragOver)
      document.removeEventListener('dragleave', onDragLeave)
      document.removeEventListener('drop', onDrop)
    }
  }, [])

  const handlePickFile = useCallback(async () => {
    try {
      setOpenError(null)
      const result = await bridge.pickFile()
      if (result.cancelled) return

      if (result.needsEncryption && result.path) {
        // Encrypted footer detected — prompt for keys before opening
        setPendingEncryptedPath(result.path)
        setShowEncryption(true)
        setFileInfo(null)
        setEncryptionConfig(undefined)
        return
      }

      if (result.error && result.path) {
        // Open failed (e.g. plaintext-footer encryption) — show error and prompt for keys
        setPendingEncryptedPath(result.path)
        setOpenError(result.error)
        setShowEncryption(true)
        setFileInfo(null)
        setEncryptionConfig(undefined)
        return
      }

      if (result.file) {
        setPendingEncryptedPath(null)
        setFileInfo(result.file)
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      setOpenError(msg)
      showError('Failed to pick file', msg)
    }
  }, [])

  const handleEncryptionApply = useCallback(async (config: EncryptionConfig | undefined) => {
    setEncryptionConfig(config)
    setOpenError(null)

    const pathToOpen = pendingEncryptedPath ?? fileInfo?.path
    if (!pathToOpen) {
      setShowEncryption(false)
      return
    }

    try {
      const updated = await bridge.openFile(pathToOpen, config)
      setFileInfo(updated)
      setPendingEncryptedPath(null)
      setShowEncryption(false)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      setOpenError(msg)
      showError('Failed to open file with encryption', msg)
    }
  }, [fileInfo, pendingEncryptedPath])

  const fileName = fileInfo?.path.split(/[\\/]/).pop() ?? null

  return (
    <div className="flex flex-col h-screen">
      {/* Header */}
      <header className="flex items-center gap-3 px-4 py-2 border-b bg-card">
        <Button variant="outline" size="sm" onClick={() => void handlePickFile()}>
          <FolderOpen className="h-4 w-4 mr-1.5" />
          Open File
        </Button>
        {(pendingEncryptedPath || fileInfo?.isEncrypted) && (
          <Button
            variant={encryptionConfig ? 'default' : 'outline'}
            size="sm"
            onClick={() => setShowEncryption(!showEncryption)}
          >
            <Lock className="h-4 w-4 mr-1.5" />
            Encryption
            {encryptionConfig && (
              <Badge variant="secondary" className="ml-1.5 text-[10px] px-1 py-0">ON</Badge>
            )}
          </Button>
        )}

        <Separator orientation="vertical" className="h-6" />

        {pendingEncryptedPath && !fileInfo ? (
          <div className="flex items-center gap-2 text-sm">
            <Badge variant="destructive" className="text-[10px] shrink-0">
              <Lock className="h-3 w-3 mr-0.5" /> Encrypted
            </Badge>
            <span className="text-muted-foreground truncate max-w-[300px]" title={pendingEncryptedPath}>
              {pendingEncryptedPath.split(/[\\/]/).pop()}
            </span>
            <span className="text-muted-foreground text-xs">— provide decryption keys to open</span>
          </div>
        ) : fileInfo ? (
          <div className="flex items-center gap-3 text-sm overflow-hidden">
            <span className="font-medium truncate max-w-[300px]" title={fileInfo.path}>
              {fileName}
            </span>
            <Badge variant="outline" className="text-[10px] shrink-0">
              {formatBytes(fileInfo.fileSize)}
            </Badge>
            <Badge variant="outline" className="text-[10px] shrink-0">
              {formatNumber(fileInfo.totalRowCount)} rows
            </Badge>
            <Badge variant="outline" className="text-[10px] shrink-0">
              {fileInfo.rowGroupCount} RG{fileInfo.rowGroupCount !== 1 ? 's' : ''}
            </Badge>
            {fileInfo.isEncrypted && (
              <Badge variant="destructive" className="text-[10px] shrink-0">
                <Lock className="h-3 w-3 mr-0.5" /> Encrypted
              </Badge>
            )}
          </div>
        ) : (
          <span className="text-sm text-muted-foreground">No file open</span>
        )}
      </header>

      {/* Body */}
      <div className="flex flex-1 overflow-hidden">
        {/* Encryption side panel */}
        {showEncryption && (
          <div className="w-[350px] border-r">
            <EncryptionPanel
              onApply={(c) => void handleEncryptionApply(c)}
              onClose={() => setShowEncryption(false)}
              error={openError}
            />
          </div>
        )}

        {/* Main content */}
        <Tabs
          value={activeTab}
          onValueChange={setActiveTab}
          className="flex-1 gap-0 overflow-hidden"
        >
          <div className="border-b px-4">
            <TabsList variant="line" className="h-9">
              <TabsTrigger value="data" className="text-xs gap-1.5">
                <Table2 className="h-3.5 w-3.5" /> Data
              </TabsTrigger>
              <TabsTrigger value="metadata" className="text-xs gap-1.5">
                <FileText className="h-3.5 w-3.5" /> Metadata
              </TabsTrigger>
              <TabsTrigger value="indices" className="text-xs gap-1.5">
                <Hash className="h-3.5 w-3.5" /> Indices
              </TabsTrigger>
              <TabsTrigger value="query" className="text-xs gap-1.5">
                <Search className="h-3.5 w-3.5" /> Query
              </TabsTrigger>
            </TabsList>
          </div>

          <TabsContent value="data" className="flex-1 overflow-hidden">
            <DataTable fileInfo={fileInfo} />
          </TabsContent>

          <TabsContent value="metadata" className="flex-1 overflow-hidden">
            <MetadataViewer fileInfo={fileInfo} />
          </TabsContent>

          <TabsContent value="indices" className="flex-1 overflow-hidden">
            <IndicesViewer fileInfo={fileInfo} />
          </TabsContent>

          <TabsContent value="query" className="flex-1 overflow-hidden" keepMounted>
            <QueryEditor fileInfo={fileInfo} />
          </TabsContent>
        </Tabs>
      </div>

      {/* Drag-and-drop overlay */}
      {dragOver && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-background/80 backdrop-blur-sm border-2 border-dashed border-primary/50 pointer-events-none">
          <div className="flex flex-col items-center gap-3 text-primary">
            <FolderOpen className="h-12 w-12" />
            <p className="text-lg font-medium">Drop a Parquet file to open</p>
          </div>
        </div>
      )}
    </div>
  )
}
