import { serve } from '@hono/node-server'
import { Pool } from 'pg'
import { createApp } from './app.js'

if (!process.env.DATABASE_URL) {
  throw new Error('Set DATABASE_URL to a leased database connection through PgTest')
}

const pool = new Pool({ connectionString: process.env.DATABASE_URL })
const app = createApp(pool)

serve({
  fetch: app.fetch,
  port: 3000
}, (info) => {
  console.log(`Server is running on http://localhost:${info.port}`)
})
