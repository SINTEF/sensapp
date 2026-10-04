import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// The API lives at the root of SensApp; `npm run dev` proxies it.
const SENSAPP = 'http://localhost:3000'
const API_PATHS = ['/api', '/health', '/metrics', '/series', '/docs']

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  // SensApp serves the built files under /ui/ (src/http/ui.rs), and `npm run dev` does too.
  base: '/ui/',
  build: {
    // echarts alone is about 580 kB, and only loaded when a chart is drawn.
    chunkSizeWarningLimit: 650,
  },
  server: {
    proxy: Object.fromEntries(
      API_PATHS.map((path) => [path, { target: SENSAPP, changeOrigin: true }]),
    ),
  },
})
