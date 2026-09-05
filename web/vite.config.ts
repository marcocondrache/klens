import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/health': 'http://localhost:8080',
      '/api': 'http://localhost:8080',
    },
  },
  build: {
    outDir: '../crates/klens-server/static',
    emptyOutDir: true,
  },
})
