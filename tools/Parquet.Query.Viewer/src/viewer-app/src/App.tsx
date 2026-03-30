import { useCallback, useState } from 'react'
import { bridge } from '@/api/bridge'
import type { EncryptionConfig, ParquetFileInfo } from '@/api/types'
import { DataTable } from '@/components/DataTable'
import { MetadataViewer } from '@/components/MetadataViewer'
import { QueryEditor } from '@/components/QueryEditor'
import { EncryptionPanel } from '@/components/EncryptionPanel'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { formatBytes, formatNumber } from '@/lib/utils'
import {
  FolderOpen,
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

  const handlePickFile = useCallback(async () => {
    try {
      const result = await bridge.pickFile()
      if (!result.cancelled && result.file) {
        setFileInfo(result.file)
      }
    } catch (err) {
      console.error('Failed to pick file:', err)
    }
  }, [])

  const handleEncryptionApply = useCallback(async (config: EncryptionConfig | undefined) => {
    setEncryptionConfig(config)
    setShowEncryption(false)

    if (fileInfo) {
      try {
        const updated = await bridge.openFile(fileInfo.path, config)
        setFileInfo(updated)
      } catch (err) {
        console.error('Failed to reopen file with encryption:', err)
      }
    }
  }, [fileInfo])

  const fileName = fileInfo?.path.split(/[\\/]/).pop() ?? null

  return (
    <div className="flex flex-col h-screen">
      {/* Header */}
      <header className="flex items-center gap-3 px-4 py-2 border-b bg-card">
        <Button variant="outline" size="sm" onClick={() => void handlePickFile()}>
          <FolderOpen className="h-4 w-4 mr-1.5" />
          Open File
        </Button>
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

        <Separator orientation="vertical" className="h-6" />

        {fileInfo ? (
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
            />
          </div>
        )}

        {/* Main content */}
        <Tabs
          value={activeTab}
          onValueChange={setActiveTab}
          className="flex-1 flex flex-col overflow-hidden"
        >
          <div className="border-b px-4">
            <TabsList className="h-9">
              <TabsTrigger value="data" className="text-xs gap-1.5">
                <Table2 className="h-3.5 w-3.5" /> Data
              </TabsTrigger>
              <TabsTrigger value="metadata" className="text-xs gap-1.5">
                <FileText className="h-3.5 w-3.5" /> Metadata
              </TabsTrigger>
              <TabsTrigger value="query" className="text-xs gap-1.5">
                <Search className="h-3.5 w-3.5" /> Query
              </TabsTrigger>
            </TabsList>
          </div>

          <TabsContent value="data" className="flex-1 overflow-hidden mt-0">
            <DataTable fileInfo={fileInfo} />
          </TabsContent>

          <TabsContent value="metadata" className="flex-1 overflow-hidden mt-0">
            <MetadataViewer fileInfo={fileInfo} />
          </TabsContent>

          <TabsContent value="query" className="flex-1 overflow-hidden mt-0">
            <QueryEditor fileInfo={fileInfo} />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}
