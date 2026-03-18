import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { fileURLToPath } from 'node:url'

const FRONTEND_ROOT = fileURLToPath(new URL('.', import.meta.url))

export default defineConfig({
  root: FRONTEND_ROOT,
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
    },
  },
})
