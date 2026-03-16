import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/upload': 'http://localhost:8000',
      '/start': 'http://localhost:8000',
      '/stop': 'http://localhost:8000',
      '/metrics': 'http://localhost:8000',
    }
  }
})
