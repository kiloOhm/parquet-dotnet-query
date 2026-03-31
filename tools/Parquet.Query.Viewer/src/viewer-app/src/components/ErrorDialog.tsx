import { createContext, useCallback, useContext, useState } from 'react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Copy, Check } from 'lucide-react'

interface ErrorState {
  title: string
  message: string
}

type ShowError = (title: string, message: string) => void

const ErrorContext = createContext<ShowError>(() => {})

export function useError(): ShowError {
  return useContext(ErrorContext)
}

export function ErrorProvider({ children }: { children: React.ReactNode }) {
  const [error, setError] = useState<ErrorState | null>(null)
  const [copied, setCopied] = useState(false)

  const showError: ShowError = useCallback((title, message) => {
    setError({ title, message })
    setCopied(false)
  }, [])

  const handleCopy = useCallback(async () => {
    if (!error) return
    const text = `${error.title}\n\n${error.message}`
    await navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }, [error])

  return (
    <ErrorContext.Provider value={showError}>
      {children}
      <Dialog open={error !== null} onOpenChange={(open) => { if (!open) setError(null) }}>
        <DialogContent className="max-w-lg max-h-[80vh] flex flex-col sm:max-w-lg">
          <DialogHeader>
            <DialogTitle className="text-destructive">{error?.title ?? 'Error'}</DialogTitle>
            <DialogDescription>An error occurred</DialogDescription>
          </DialogHeader>
          <ScrollArea className="flex-1 min-h-0">
            <pre className="text-xs font-mono whitespace-pre-wrap break-words p-3 bg-muted rounded-md">
              {error?.message ?? ''}
            </pre>
          </ScrollArea>
          <div className="flex justify-end">
            <Button variant="outline" size="sm" onClick={() => void handleCopy()}>
              {copied ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
              {copied ? 'Copied' : 'Copy to clipboard'}
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </ErrorContext.Provider>
  )
}
