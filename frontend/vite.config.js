import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173, // Vite dev server port
    host: 'localhost'
  },
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
  }
})
