import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router-dom'
import './index.css'
import App from './App.tsx'
import { configureApiClient } from './api/clientConfig'
import { createQueryClient } from './api/queryClient'
import { pickUpTokenFromAddress } from './lib/tokenFromAddress'

// Configure API client
configureApiClient()

// A link printed by a local SensApp carries a token in its fragment
pickUpTokenFromAddress()

const queryClient = createQueryClient()

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter basename={import.meta.env.BASE_URL}>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
)
