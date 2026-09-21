import { drizzle } from 'drizzle-orm/node-postgres'
import { Hono } from 'hono'
import type { Pool } from 'pg'
import { users } from './schema.js'

export function createApp(pool: Pool) {
  const db = drizzle({ client: pool })
  const app = new Hono()

  app.get('/users', async (c) => {
    return c.json(await db.select().from(users).orderBy(users.id))
  })

  app.post('/users', async (c) => {
    const body = await c.req.json<{ id: number; name: string }>()
    if (!Number.isInteger(body.id) || typeof body.name !== 'string' || !body.name.trim()) {
      return c.json({ error: 'An integer id and a nonempty name are required' }, 400)
    }
    const [user] = await db.insert(users).values({ id: body.id, name: body.name }).returning()
    return c.json(user, 201)
  })

  return app
}
