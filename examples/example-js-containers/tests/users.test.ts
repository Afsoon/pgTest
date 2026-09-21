import { randomUUID } from 'node:crypto'
import { fileURLToPath } from 'node:url'
import { Client, Pool } from 'pg'
import { GenericContainer, Network, Wait } from 'testcontainers'
import type { StartedNetwork, StartedTestContainer } from 'testcontainers'
import { afterAll, beforeAll, describe, test } from 'vitest'
import { createApp } from '../src/app.js'

let network: StartedNetwork | undefined
let postgres: StartedTestContainer | undefined
let pgtest: StartedTestContainer | undefined
let control: Pool | undefined

beforeAll(async () => {
  const pgtestContainer = process.env.PGTEST_IMAGE
    ? new GenericContainer(process.env.PGTEST_IMAGE)
    : await GenericContainer
      .fromDockerfile(fileURLToPath(new URL('../../../', import.meta.url)))
      .withBuildkit()
      .build()

  network = await new Network().start()
  postgres = await new GenericContainer('postgres:18-alpine')
    .withCommand([
      'postgres',
      '-c', 'file_copy_method=clone',
      '-c', 'fsync=off',
      '-c', 'synchronous_commit=off',
      '-c', 'full_page_writes=off',
      '-c', 'wal_level=minimal',
      '-c', 'max_wal_senders=0',
      '-c', 'archive_mode=off',
      '-c', 'summarize_wal=off',
      '-c', 'autovacuum=off',
      '-c', 'random_page_cost=1.1',
    ])
    .withNetwork(network)
    .withNetworkAliases('postgres')
    .withEnvironment({
      POSTGRES_DB: 'test_template',
      POSTGRES_USER: 'postgres',
      POSTGRES_PASSWORD: 'postgres',
      POSTGRES_HOST_AUTH_METHOD: 'trust',
    })
    .withExposedPorts(5432)
    .withWaitStrategy(Wait.forSuccessfulCommand('pg_isready -h 127.0.0.1 -U postgres -d test_template'))
    .withStartupTimeout(90_000)
    .start()

  const template = new Client({
    host: postgres.getHost(),
    port: postgres.getMappedPort(5432),
    user: 'postgres',
    database: 'test_template',
    connectionTimeoutMillis: 5_000,
  })
  try {
    await template.connect()
    await template.query('CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL)')
    await template.query('INSERT INTO users (id, name) VALUES ($1, $2)', [0, 'Seed user'])
  } finally {
    // PostgreSQL cannot clone the template while this connection is open.
    await template.end()
  }

  pgtest = await pgtestContainer
    .withNetwork(network)
    .withEnvironment({
      PGTEST_PG_HOST: 'postgres',
      PGTEST_PG_PORT: '5432',
      PGTEST_PG_USER: 'postgres',
      PGTEST_PG_DATABASE: 'test_template',
      PGTEST_LISTEN_ADDR: '0.0.0.0',
      PGTEST_LISTEN_PORT: '6432',
      PGTEST_LEASE_CLAIM_TIMEOUT_MS: '120000',
    })
    .withExposedPorts(6432)
    .withWaitStrategy(Wait.forLogMessage(/pgtest server listening/))
    .withStartupTimeout(90_000)
    .start()

  control = new Pool(connectionOptions('pgtest'))
  await control.query('SELECT 1')
}, 600_000)

afterAll(async () => {
  try {
    await control?.end()
  } finally {
    try {
      await pgtest?.stop()
    } finally {
      try {
        await postgres?.stop()
      } finally {
        await network?.stop()
      }
    }
  }
})

function connectionOptions(database: string) {
  if (!pgtest) throw new Error('PgTest has not started')
  return {
    host: pgtest.getHost(),
    port: pgtest.getMappedPort(6432),
    user: 'postgres',
    database,
    max: 2,
    connectionTimeoutMillis: 5_000,
  }
}

async function withLease(run: (pool: Pool, leaseId: string) => Promise<void>) {
  const leaseId = randomUUID()
  const pool = new Pool(connectionOptions(`test_template/${leaseId}`))
  try {
    await run(pool, leaseId)
  } finally {
    try {
      await pool.end()
    } finally {
      await control!.query('SELECT pgtest_release($1::text)', [leaseId])
    }
  }
}

describe.concurrent('users in isolated PgTest databases', () => {
  test.for(['Ada', 'Grace'])('creates user 1 named %s without affecting other tests', async (name, { expect }) => {
    await withLease(async (pool) => {
      const app = createApp(pool)
      const initial = await app.request('/users')
      expect(await initial.json()).toEqual([{ id: 0, name: 'Seed user' }])

      const created = await app.request('/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: 1, name }),
      })
      expect(created.status).toBe(201)
      expect(await created.json()).toEqual({ id: 1, name })

      const response = await app.request('/users')
      expect(await response.json()).toEqual([{ id: 0, name: 'Seed user' }, { id: 1, name }])
    })
  })

  test('shares committed data between connections using the same lease', async ({ expect }) => {
    await withLease(async (pool, leaseId) => {
      await pool.query('INSERT INTO users (id, name) VALUES ($1, $2)', [1, 'Shared user'])
      const secondPool = new Pool(connectionOptions(`test_template/${leaseId}`))
      try {
        const response = await createApp(secondPool).request('/users')
        expect(await response.json()).toEqual([
          { id: 0, name: 'Seed user' },
          { id: 1, name: 'Shared user' },
        ])
      } finally {
        await secondPool.end()
      }
    })
  })

  test('rejects new connections after releasing a lease', async ({ expect }) => {
    const leaseId = randomUUID()
    const options = connectionOptions(`test_template/${leaseId}`)
    const first = new Client(options)
    try {
      try {
        await first.connect()
        await first.query('SELECT 1')
      } finally {
        await first.end()
      }
      const released = await control!.query('SELECT pgtest_release($1::text)', [leaseId])
      expect(released.rows[0].pgtest_release).toBe(true)

      const second = new Client(options)
      try {
        await expect(second.connect()).rejects.toMatchObject({ code: '55000' })
      } finally {
        await second.end()
      }
    } finally {
      await control!.query('SELECT pgtest_release($1::text)', [leaseId])
    }
  })
})
