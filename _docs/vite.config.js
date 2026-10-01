import { defineConfig } from 'vite'

// GitHub Pages serves the site from /<repository>/.
export default defineConfig({
  base: process.env.DOCS_BASE ?? '/kafka-mcp/',
})
