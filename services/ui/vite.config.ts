import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    // Only used by `npm run dev`. In the container nginx does the proxying.
    proxy: {
      '/api': 'http://localhost:3000',
      '/metrics': 'http://localhost:3000',
      '/consumer': { target: 'http://localhost:3001', rewrite: (p) => p.replace(/^\/consumer/, '') },
    },
  },
});
