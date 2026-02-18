
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// VITE_API_URL is injected at build time via environment variable or .env file.
// The app reads it at runtime via import.meta.env.VITE_API_URL.
// For Vercel deployments, set VITE_API_URL in the Vercel project settings.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    // Proxy API/WS calls to the backend in local dev so CORS never matters
    proxy: {
      '/api': {
        target: process.env.VITE_API_URL || 'http://localhost:8000',
        changeOrigin: true,
      },
      '/ws': {
        target: (process.env.VITE_API_URL || 'http://localhost:8000').replace(/^http/, 'ws'),
        ws: true,
        changeOrigin: true,
      },
    },
  },
})
