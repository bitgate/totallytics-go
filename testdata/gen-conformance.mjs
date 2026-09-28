// Replays scripted requests through the JavaScript SDK and prints what it
// sends, for conformance_test.go. From the repo root:
//   node testdata/gen-conformance.mjs ../totallytics-js > testdata/conformance.json
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

const root = process.argv[2]
if (!root) {
  console.error('usage: node testdata/gen-conformance.mjs <path to totallytics-js>')
  process.exit(2)
}
const { Totallytics, VERSION, bucket } = await import(pathToFileURL(join(root, 'dist', 'index.js')).href)

const KEY = 'tt_' + 'ab'.repeat(24)
const MINUTE = Date.UTC(2026, 8, 26, 12, 0, 0)
const at = (ms) => MINUTE + ms

let sent = []
globalThis.fetch = async (_url, init) => {
  sent.push(JSON.parse(init.body))
  return new Response('{"accepted":{"metrics":1,"errors":0},"rejected":0}', { status: 202 })
}

async function replay(name, entries, maxBatchRows = 1000) {
  sent = []
  const client = new Totallytics({ apiKey: KEY, endpoint: 'https://ingest.invalid/api/ingest', maxBatchRows })
  for (const { error, ...entry } of entries) {
    client.record(error === undefined ? entry : { ...entry, error: new Error(error) })
  }
  await client.flush()
  return { name, max_batch_rows: maxBatchRows, entries, batches: sent.map(({ metrics, errors }) => ({ metrics, errors })) }
}

const a = (n) => 'a'.repeat(n)
const request = (fields) => ({ method: 'GET', path: '/', status: 200, durationMs: 1, startedAt: at(0), ...fields })

const cases = [
  await replay('aggregation', [
    request({ path: '/users/1', route: '/users/:id', durationMs: 12.5, startedAt: at(1_000), userAgent: 'curl/8.5.0' }),
    request({ method: 'get', path: '/users/2', route: '/users/:id', durationMs: 7.25, startedAt: at(30_000), userAgent: 'curl/8.5.0' }),
    request({ path: '/users/3', route: '/users/:id', durationMs: 0.1, startedAt: at(59_999), userAgent: 'curl/8.5.0' }),
    request({ path: '/users/4', route: '/users/:id', durationMs: 0.2, startedAt: at(60_000), userAgent: 'curl/8.5.0' }),
    request({ path: '/users/5', route: '/users/:id', durationMs: 3, startedAt: at(-1), userAgent: 'curl/8.5.0' }),
    request({ method: 'post', path: '/orders', status: 201, durationMs: 48.123456, startedAt: at(2_000), consumer: 'acme' }),
    request({ method: '', path: '/orders', status: 201, durationMs: 3, startedAt: at(3_000), consumer: 'acme' }),
    request({ method: 'Delete', path: '/orders/9', route: '/orders/:id', status: 204, durationMs: 0.1, userAgent: 'python-requests/2.32.3', consumer: 'acme' }),
    request({ method: 'DELETE', path: '/orders/10', route: '/orders/:id', status: 204, durationMs: 0.2, userAgent: 'python-requests/2.32.3', consumer: 'acme' }),
    request({ path: '', status: 200, durationMs: 5 }),
    request({ path: '/health', status: 200, durationMs: 1, userAgent: '', consumer: '' }),
  ]),
  await replay('durations', [0, -3, 0.0004, 0.0005, 0.0015, 1, 1.08, 1.09, 2.5, 12.3456789, 999.9995, 1234.5675, 60_000, 3_600_000.123].flatMap((durationMs, i) => [
    request({ path: '/sum', durationMs }),
    request({ path: `/err/${i}`, route: '/err/:n', status: 500, durationMs, error: `failed after ${durationMs}ms` }),
  ])),
  await replay('clipping', [
    request({ path: '/' + a(600), route: a(511) + '😀', status: 500, userAgent: a(511) + '😀tail', consumer: a(127) + '😀x', error: a(999) + '😀more' }),
    request({ path: '/' + a(510) + '😀', route: a(510) + '😀', status: 404, userAgent: a(510) + '😀tail', consumer: a(126) + '😀x', error: a(998) + '😀x' }),
    request({ path: '/unicode', route: '/' + '日本'.repeat(300), userAgent: 'é'.repeat(600), consumer: 'ü'.repeat(200) }),
  ]),
  await replay('statuses', [99, 100, 199, 200, 399, 400, 499, 500, 599, 600].map((status, i) => request({ path: `/s/${status}`, status, durationMs: i + 1, error: status >= 400 ? `status ${status}` : undefined }))),
  await replay('samples', Array.from({ length: 90 }, (_, i) => {
    const status = i % 3 === 0 ? [400, 404, 429][i % 9 / 3] : [500, 502, 503, 504][i % 4]
    return request({ path: `/fail/${i}`, route: '/fail/:n', status, durationMs: i / 7, startedAt: at(i * 500), error: `failure ${i}` })
  })),
  await replay('chunking', [
    request({ path: '/c/0', status: 500, error: 'first' }),
    request({ path: '/c/0', status: 500, durationMs: 2, error: 'second' }),
    request({ path: '/c/1', status: 404, error: 'missing' }),
    request({ path: '/c/2' }),
    request({ path: '/c/3' }),
    request({ path: '/c/4' }),
    request({ path: '/c/5' }),
    request({ path: '/c/6' }),
  ], 3),
]

const view = new DataView(new ArrayBuffer(8))
const step = (x, units) => {
  view.setFloat64(0, x)
  view.setBigUint64(0, view.getBigUint64(0) + BigInt(units))
  return view.getFloat64(0)
}
const mulberry32 = (seed) => () => {
  seed = (seed + 0x6d2b79f5) | 0
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296
}

const values = [NaN, Infinity, -Infinity, -1, 0, 0.5, 1, step(1, 1)]
for (let k = 1; k <= 260; k++) {
  const p = Math.pow(1.08, k)
  values.push(step(p, -2), step(p, -1), p, step(p, 1), step(p, 2))
}
for (let ms = 2; ms <= 3000; ms++) values.push(ms)
const random = mulberry32(20260926)
for (let i = 0; i < 2000; i++) values.push(1 + random() * 1e5)
for (let i = 0; i < 2000; i++) values.push(Math.exp(random() * 20))
for (let i = 0; i < 2000; i++) values.push(Math.floor(random() * 5e9) / 1e6)

const encode = (ms) => (Number.isFinite(ms) ? ms : String(ms))
const buckets = values.map((ms) => [encode(ms), bucket(ms)])

const lines = ['{', `  "sdk": ${JSON.stringify('totallytics-js/' + VERSION)},`, '  "buckets": [']
buckets.forEach((pair, i) => lines.push('    ' + JSON.stringify(pair) + (i < buckets.length - 1 ? ',' : '')))
lines.push('  ],', '  "cases": [')
cases.forEach((c, i) => lines.push('    ' + JSON.stringify(c) + (i < cases.length - 1 ? ',' : '')))
lines.push('  ]', '}')
process.stdout.write(lines.join('\n') + '\n')
