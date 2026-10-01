import { defineConfig } from 'vite'

// GitHub Pages serves the site from /<repository>/.
export default defineConfig({
  base: process.env.DOCS_BASE ?? '/kafka-mcp/',
  build: {
    // Inline fonts only; keep Vite's default size limit for other assets.
    assetsInlineLimit: (filePath) => filePath.endsWith('.woff2') ? true : undefined,
  },
})
