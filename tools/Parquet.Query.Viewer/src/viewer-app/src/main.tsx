import React from 'react'
import ReactDOM from 'react-dom/client'
import { TooltipProvider } from '@/components/ui/tooltip'
import { ErrorProvider } from '@/components/ErrorDialog'
import App from './App'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <TooltipProvider>
      <ErrorProvider>
        <App />
      </ErrorProvider>
    </TooltipProvider>
  </React.StrictMode>,
)
