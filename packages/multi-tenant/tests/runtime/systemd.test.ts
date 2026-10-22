import { request } from 'node:http'
import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SystemdRuntimeProvider } from '../../src/runtime/providers/systemd.ts'

const fake = fileURLToPath(new URL('./systemctl.fixture.mjs', import.meta.url))
const domainId = '00000000-0000-4000-8000-000000000001'
const spec = { domainId, generation: 1, version: '0.1.5-rc.2' }
let root: string
let units: string

function provider(mode = '') {
  return new SystemdRuntimeProvider({ directory: root, systemctl: [process.execPath, fake, units, root, mode], pollMs: 20 })
}

function get(socketPath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    request({ socketPath, path: '/' }, response => {
      let body = ''
      response.on('data', chunk => { body += chunk })
      response.on('end', () => resolve(body))
    }).on('error', reject).end()
  })
}

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), 'dsh-sd-'))
  units = await mkdtemp(join(tmpdir(), 'dsh-units-'))
  await mkdir(join(root, 'profiles', domainId), { recursive: true })
  await writeFile(join(root, 'profiles', domainId, 'runtime.patch.json'), '{}')
})

afterEach(async () => {
  // Leave no fixture runtime alive even when an assertion failed mid-test.
  await provider().recover(spec).catch(() => {})
  await rm(root, { recursive: true, force: true })
  await rm(units, { recursive: true, force: true })
})

describe('Linux systemd runtime provider', () => {
  it('serves through the private socket, stops idempotently and releases the unit and its records', async () => {
    const handle = provider().acquire(spec, new AbortController().signal)
    const ready = await handle.ready
    expect(ready.endpoint).toBe('http://127.0.0.1:3081')
    expect(ready.authentication).toEqual({ cookie: 'session=probe' })
    expect(await get(ready.socketPath!)).toBe(domainId)
    expect(await readFile(join(root, 'spec', `${domainId}.env`), 'utf8')).toContain('DSH_RUNTIME_GENERATION=1')
    await Promise.all([handle.stop(), handle.stop()])
    await handle.exited
    await expect(get(join(root, 'control', domainId, 'http.sock'))).rejects.toThrow()
    expect(existsSync(join(root, 'control', domainId))).toBe(false)
    expect(existsSync(join(root, 'spec', `${domainId}.env`))).toBe(false)
  })

  it('refuses to adopt an active unit and requires verified recovery', async () => {
    const first = provider().acquire(spec, new AbortController().signal)
    await first.ready
    const second = provider().acquire({ ...spec, generation: 2 }, new AbortController().signal)
    await expect(second.ready).rejects.toThrow('verified recovery required')
    await second.stop()
    expect(await get((await first.ready).socketPath!)).toBe(domainId)
    await first.stop()
  })

  it('rejects readiness for another generation and still stops the unit', async () => {
    const handle = provider('wrong-generation').acquire(spec, new AbortController().signal)
    await expect(handle.ready).rejects.toThrow('readiness mismatch')
    await handle.stop()
    await handle.exited
    expect(existsSync(join(units, `dsh-domain@${domainId}.service.json`))).toBe(false)
  })

  it('reports an exit before readiness and an unexpected exit after it', async () => {
    const early = provider('exit-before-ready').acquire(spec, new AbortController().signal)
    await expect(early.ready).rejects.toThrow('exited before readiness')
    await early.exited
    await early.stop()
    const crashing = provider('crash-after-ready').acquire(spec, new AbortController().signal)
    await crashing.ready
    await crashing.exited
    await crashing.stop()
  })

  it('stops a unit that never becomes ready when stopped before readiness', async () => {
    const handle = provider('never-ready').acquire(spec, new AbortController().signal)
    const rejected = expect(handle.ready).rejects.toThrow('cancelled')
    await new Promise(resolve => setTimeout(resolve, 200))
    await handle.stop()
    await rejected
    expect(existsSync(join(units, `dsh-domain@${domainId}.service.json`))).toBe(false)
  })

  it('recovers its own generation and refuses a foreign one', async () => {
    const handle = provider().acquire({ ...spec, generation: 3 }, new AbortController().signal)
    await handle.ready
    await expect(provider().recover({ ...spec, generation: 2 })).rejects.toThrow('foreign runtime generation')
    await provider().recover({ ...spec, generation: 3 })
    await handle.exited
    expect(existsSync(join(root, 'control', domainId))).toBe(false)
    const next = provider().acquire({ ...spec, generation: 4 }, new AbortController().signal)
    expect(await get((await next.ready).socketPath!)).toBe(domainId)
    await next.stop()
  })

  it('refuses to stop a unit that a newer acquire now owns', async () => {
    const stale = provider().acquire(spec, new AbortController().signal)
    await stale.ready
    await provider().recover(spec)
    const owner = provider().acquire({ ...spec, generation: 2 }, new AbortController().signal)
    await owner.ready
    await expect(stale.stop()).rejects.toThrow('ownership changed')
    expect(await get((await owner.ready).socketPath!)).toBe(domainId)
    await owner.stop()
  })

  it('tolerates a transient systemctl failure without reporting an exit', async () => {
    const handle = provider('show-fails').acquire(spec, new AbortController().signal)
    await handle.ready
    await writeFile(join(units, 'show-fails'), '')
    await new Promise(resolve => setTimeout(resolve, 30))
    await rm(join(units, 'show-fails'))
    const settled = await Promise.race([handle.exited.then(() => 'exited'), new Promise(resolve => setTimeout(() => resolve('running'), 200))])
    expect(settled).toBe('running')
    await handle.stop()
  })

  it('rejects an unprovisioned profile, an invalid domain id and an overlong directory before starting', async () => {
    const other = '00000000-0000-4000-8000-000000000002'
    const unprovisioned = provider().acquire({ ...spec, domainId: other }, new AbortController().signal)
    await expect(unprovisioned.ready).rejects.toThrow('profile is not provisioned')
    await unprovisioned.stop()
    const invalid = provider().acquire({ ...spec, domainId: '../escape' }, new AbortController().signal)
    await expect(invalid.ready).rejects.toThrow('Invalid domain id')
    await invalid.stop()
    const long = new SystemdRuntimeProvider({ directory: join(root, 'long-platform-directory-'.repeat(5)), systemctl: [process.execPath, fake, units, root, ''] })
    const overlong = long.acquire(spec, new AbortController().signal)
    await expect(overlong.ready).rejects.toThrow('too long for a Linux Unix socket')
    await overlong.stop()
    expect(existsSync(join(units, `dsh-domain@${other}.service.json`))).toBe(false)
  })

  it('validates its configuration', () => {
    expect(() => new SystemdRuntimeProvider({ directory: root, unit: 'bad unit' })).toThrow('Invalid unit template name')
    expect(() => new SystemdRuntimeProvider({ directory: root, systemctl: [] })).toThrow('must not be empty')
    expect(() => new SystemdRuntimeProvider({ directory: root, pollMs: 0 })).toThrow('Invalid unit polling interval')
  })
})
