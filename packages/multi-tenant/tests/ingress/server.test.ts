import { createServer } from 'node:http'
import type { Server } from 'node:http'
import { once } from 'node:events'
import type { AddressInfo } from 'node:net'
import { WebSocket, WebSocketServer } from 'ws'
import { afterEach, expect, it } from 'vitest'
import { MemoryDomainSessions } from '../../src/ingress/authentication.ts'
import { createDomainIngress } from '../../src/ingress/server.ts'

const cleanups: (() => Promise<unknown> | void)[] = []
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})
async function listen(server: Server) {
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`
}

async function setup() {
  const native = createServer((req, res) => {
    if (req.headers.cookie !== 'native=private') { res.writeHead(401).end(); return }
    res.setHeader('set-cookie', 'native=never-expose')
    res.setHeader('content-security-policy', "script-src 'self'")
    if (req.url === '/stream') { res.writeHead(200); res.write('first'); return }
    if (req.url === '/echo') { req.pipe(res); return }
    res.end(JSON.stringify(req.headers))
  })
  const ws = new WebSocketServer({ server: native })
  ws.on('connection', socket => { socket.on('message', data => socket.send(data)) })
  const endpoint = await listen(native)
  cleanups.push(async () => {
    for (const socket of ws.clients) socket.terminate()
    ws.close()
    native.closeAllConnections()
    await new Promise<void>(resolve => native.close(() => resolve()))
  })
  const sessions = new MemoryDomainSessions('domain-session')
  cleanups.push(() => sessions.close())
  const alice = { tenantId: 'acme', principalId: 'alice' }
  const bob = { tenantId: 'acme', principalId: 'bob' }
  const a = sessions.issue(alice)
  const b = sessions.issue(bob)
  const runtime = new AbortController()
  let calls = 0
  let origin = ''
  const ingress = createDomainIngress({
    authenticator: sessions,
    originFor: owner => owner.principalId === 'alice' ? origin : 'https://bob.example',
    runtime: { ensure: async () => { calls++; return {
      domainId: 'domain-a', generation: 1, revision: 0, version: '0.1.5-rc.2', endpoint,
      signal: runtime.signal, authentication: { cookie: 'native=private' },
    } } },
  })
  origin = await listen(ingress.server)
  cleanups.push(() => ingress.close())
  const headers = { cookie: `domain-session=${a}`, origin }
  return { origin, headers, sessions, a, b, runtime, getCalls: () => calls, ingress }
}

it('rejects missing/duplicate/cross-owner cookies and cross-origin calls before runtime lookup', async () => {
  const t = await setup()
  expect((await fetch(t.origin)).status).toBe(401)
  expect((await fetch(t.origin, { headers: { cookie: `domain-session=${t.a}; domain-session=${t.b}` } })).status).toBe(401)
  expect((await fetch(t.origin, { headers: { cookie: `domain-session=${t.b}` } })).status).toBe(403)
  expect((await fetch(t.origin, { headers: { ...t.headers, origin: 'https://evil.example' } })).status).toBe(403)
  expect((await fetch(t.origin, { method: 'POST', headers: { cookie: t.headers.cookie } })).status).toBe(403)
  expect(t.getCalls()).toBe(0)
})

it('maps internal authority, strips user credentials and does not expose native cookies', async () => {
  const t = await setup()
  const response = await fetch(t.origin, { headers: {
    ...t.headers, authorization: 'Bearer evil', 'x-forwarded-host': 'bob.example', 'x-dsh-principal': 'bob',
  } })
  expect(response.status).toBe(200)
  expect(response.headers.get('set-cookie')).toBeNull()
  expect(response.headers.get('cache-control')).toBe('no-store')
  expect(response.headers.get('content-security-policy')).toContain("script-src 'self'")
  expect(response.headers.get('content-security-policy')).toContain("frame-ancestors 'self'")
  const received = await response.json()
  expect(received.cookie).toBe('native=private')
  expect(received.origin).not.toBe(t.origin)
  expect(received.authorization).toBeUndefined()
  expect(received['x-dsh-principal']).toBeUndefined()
  expect(received['x-forwarded-host']).toBeUndefined()
})

it('streams raw binary without rewriting bytes and preserves HEAD', async () => {
  const t = await setup()
  const body = Buffer.alloc(2 * 1024 * 1024, 0xa7)
  const response = await fetch(`${t.origin}/echo`, { method: 'POST', headers: t.headers, body })
  expect(Buffer.from(await response.arrayBuffer()).equals(body)).toBe(true)
  const head = await fetch(t.origin, { method: 'HEAD', headers: t.headers })
  expect(head.status).toBe(200)
  expect(await head.text()).toBe('')
})

it('cancels a streaming HTTP response when its authentication session is revoked', async () => {
  const t = await setup()
  const response = await fetch(`${t.origin}/stream`, { headers: t.headers })
  const reader = response.body!.getReader()
  expect((await reader.read()).done).toBe(false)
  t.sessions.revoke(t.a)
  await expect(reader.read()).rejects.toThrow()
  expect((await fetch(t.origin, { headers: t.headers })).status).toBe(401)
})

it('transparently tunnels WebSocket frames and closes the old generation on invalidation', async () => {
  const t = await setup()
  const socket = new WebSocket(t.origin.replace('http:', 'ws:') + '/mux', { headers: t.headers })
  cleanups.push(() => socket.terminate())
  await once(socket, 'open')
  const received = once(socket, 'message')
  socket.send(Buffer.from([0, 255, 23, 0]))
  expect((await received)[0]).toEqual(Buffer.from([0, 255, 23, 0]))
  const closed = once(socket, 'close')
  t.runtime.abort()
  await closed
})

it('expires sessions and closes pending admission even if the authenticator never settles', async () => {
  const t = await setup()
  const token = t.sessions.issue({ tenantId: 'acme', principalId: 'alice' }, 20)
  await expect.poll(async () => (await fetch(t.origin, { headers: { cookie: `domain-session=${token}` } })).status).toBe(401)
  const ingress = createDomainIngress({ authenticator: { authenticate: () => new Promise(() => {}) },
    originFor: () => 'https://unused.example', runtime: { ensure: () => { throw new Error('must not start') } } })
  const origin = await listen(ingress.server)
  const pending = fetch(origin)
  const failed = expect(pending).rejects.toThrow()
  await new Promise(resolve => setTimeout(resolve, 25))
  await ingress.close()
  await ingress.close()
  await failed
})

async function contextFixture(context: unknown = undefined) {
  const received: { path: string | undefined; headers: Record<string, string | string[] | undefined> }[] = []
  const native = createServer((req, res) => {
    received.push({ path: req.url, headers: { ...req.headers } })
    if (req.headers['x-dsh-authenticated-context']) res.setHeader('x-dsh-authenticated-context', req.headers['x-dsh-authenticated-context']!)
    res.setHeader('x-dsh-legion-admission', 'private-upstream-admission')
    res.setHeader('x-dsh-version', 'native-protocol-version')
    res.setHeader('set-cookie', 'native=private')
    if (req.url === '/stream') { res.writeHead(200); res.write('first'); return }
    res.end('native payload')
  })
  const ws = new WebSocketServer({ server: native })
  ws.on('headers', (headers, req) => {
    received.push({ path: req.url, headers: { ...req.headers } })
    if (req.headers['x-dsh-authenticated-context']) headers.push(`x-dsh-authenticated-context: ${req.headers['x-dsh-authenticated-context']}`)
    headers.push('x-dsh-legion-admission: private-upstream-admission', 'x-dsh-version: native-protocol-version', 'set-cookie: native=private')
  })
  ws.on('connection', socket => socket.on('message', data => socket.send(data)))
  const endpoint = await listen(native)
  cleanups.push(async () => {
    for (const socket of ws.clients) socket.terminate()
    ws.close()
    native.closeAllConnections()
    await new Promise<void>(resolve => native.close(() => resolve()))
  })
  const revoked = new AbortController()
  const lifetime = new AbortController()
  const identity: { owner: { tenantId: string; principalId: string }; signal: AbortSignal; authenticatedContext?: string } = {
    owner: { tenantId: 'org', principalId: 'person' }, signal: revoked.signal,
    ...(context === undefined ? {} : { authenticatedContext: context as string }),
  }
  let origin = ''
  let calls = 0
  let duringEnsure = () => {}
  const ingress = createDomainIngress({
    authenticator: { authenticate: async () => identity },
    originFor: () => origin,
    runtime: { ensure: async () => {
      calls++
      duringEnsure()
      return { domainId: 'context-domain', generation: 1, revision: 0, version: '0.1.5-rc.2', endpoint,
        signal: lifetime.signal, authentication: { cookie: 'native=private' } }
    } },
  })
  origin = await listen(ingress.server)
  cleanups.push(() => ingress.close())
  const headers = { origin, 'x-dsh-authenticated-context': 'browser-forgery', 'x-dsh-legion-admission': 'browser-private-token', 'x-dsh-principal': 'other-person' }
  return { origin, headers, received, identity, revoked, getCalls: () => calls, setDuringEnsure: (fn: () => void) => { duringEnsure = fn } }
}

it('snapshots only authenticated context for HTTP and keeps private headers off browser responses', async () => {
  const t = await contextFixture('authenticator.signed.proof')
  t.setDuringEnsure(() => { t.identity.authenticatedContext = 'later-mutated-proof' })
  const response = await fetch(t.origin, { headers: t.headers })
  expect(await response.text()).toBe('native payload')
  expect(t.received[0]!.headers['x-dsh-authenticated-context']).toBe('authenticator.signed.proof')
  expect(t.received[0]!.headers['x-dsh-legion-admission']).toBeUndefined()
  expect(t.received[0]!.headers['x-dsh-principal']).toBeUndefined()
  expect(t.received[0]!.headers.cookie).toBe('native=private')
  expect(response.headers.get('x-dsh-authenticated-context')).toBeNull()
  expect(response.headers.get('x-dsh-legion-admission')).toBeNull()
  expect(response.headers.get('set-cookie')).toBeNull()
  expect(response.headers.get('x-dsh-version')).toBe('native-protocol-version')
})

it('does not mint context from spoofed HTTP or WebSocket headers when authenticator omits it', async () => {
  const t = await contextFixture()
  expect((await fetch(t.origin, { headers: t.headers })).status).toBe(200)
  const socket = new WebSocket(t.origin.replace('http:', 'ws:'), { headers: t.headers })
  cleanups.push(() => socket.terminate())
  await once(socket, 'open')
  expect(t.received).toHaveLength(2)
  for (const request of t.received) {
    expect(request.headers['x-dsh-authenticated-context']).toBeUndefined()
    expect(request.headers['x-dsh-legion-admission']).toBeUndefined()
  }
})

it('rejects malformed authenticator context before HTTP and WebSocket runtime lookup', async () => {
  const t = await contextFixture()
  for (const context of ['', 'has space', 'line\nbreak', '\u007f', 'é', 'x'.repeat(4097), null, 42]) {
    t.identity.authenticatedContext = context as string
    expect((await fetch(t.origin, { headers: t.headers })).status).toBe(401)
    const socket = new WebSocket(t.origin.replace('http:', 'ws:'), { headers: t.headers })
    cleanups.push(() => socket.terminate())
    const rejected = new Promise<number | undefined>((resolve, reject) => {
      socket.once('unexpected-response', (_request, response) => { response.resume(); socket.terminate(); resolve(response.statusCode) })
      socket.once('error', reject)
      socket.once('open', () => reject(new Error('Malformed context reached WebSocket runtime')))
    })
    expect(await rejected).toBe(401)
  }
  expect(t.getCalls()).toBe(0)
  expect(t.received).toHaveLength(0)
})

it('forwards bounded authenticated context on WebSocket and preserves revocation and native frames', async () => {
  const proof = 'p'.repeat(4096)
  const t = await contextFixture(proof)
  const socket = new WebSocket(t.origin.replace('http:', 'ws:') + '/mux', { headers: t.headers })
  cleanups.push(() => socket.terminate())
  const upgrade = once(socket, 'upgrade')
  await once(socket, 'open')
  const [response] = await upgrade
  expect(t.received[0]!.headers['x-dsh-authenticated-context']).toBe(proof)
  expect(t.received[0]!.headers['x-dsh-legion-admission']).toBeUndefined()
  expect(response.headers['x-dsh-authenticated-context']).toBeUndefined()
  expect(response.headers['x-dsh-legion-admission']).toBeUndefined()
  expect(response.headers['set-cookie']).toBeUndefined()
  expect(response.headers['x-dsh-version']).toBe('native-protocol-version')
  const message = once(socket, 'message')
  socket.send(Buffer.from([0, 255, 23]))
  expect((await message)[0]).toEqual(Buffer.from([0, 255, 23]))
  const closed = once(socket, 'close')
  t.revoked.abort()
  await closed
  expect((await fetch(t.origin, { headers: t.headers })).status).toBe(401)
})

it('revokes an active context-bearing HTTP stream without changing payloads', async () => {
  const t = await contextFixture('signed.context.proof')
  const response = await fetch(t.origin + '/stream', { headers: t.headers })
  const reader = response.body!.getReader()
  expect(new TextDecoder().decode((await reader.read()).value)).toBe('first')
  const pending = reader.read()
  expect(await Promise.race([pending.then(() => 'settled', () => 'settled'), new Promise(resolve => setTimeout(() => resolve('pending'), 20))])).toBe('pending')
  const rejected = expect(pending).rejects.toThrow()
  t.revoked.abort()
  await rejected
  expect(t.received[0]!.headers['x-dsh-authenticated-context']).toBe('signed.context.proof')
})
