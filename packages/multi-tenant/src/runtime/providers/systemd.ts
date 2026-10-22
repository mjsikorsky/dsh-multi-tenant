import { execFile } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { lstat, mkdir, readFile, realpath, rename, rm, writeFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { promisify } from 'node:util'
import type { RuntimeHandle, RuntimeProvider, RuntimeReady, RuntimeSpec } from '../provider.ts'

import { openPrivateSocket, readRuntimeReady } from './private-control.ts'

const exec = promisify(execFile)

export interface SystemdRuntimeOptions {
  /** Platform-owned local directory. Must equal the `directory` the host's
   * `<unit>@.service` template binds from; the provider and the unit share this
   * layout: control/<id> → /control, data/<id> → /domain, workspaces/<id> →
   * /workspace, profiles/<id> → /profile (read-only), spec/<id>.env (never bound).
   */
  readonly directory: string
  /** Template unit name without `@.service`. Default `dsh-domain`. */
  readonly unit?: string
  /** Command prefix for systemctl. Default `['systemctl']`. The caller must be
   * permitted to start, stop and reset-failed `<unit>@*.service` and nothing else. */
  readonly systemctl?: readonly string[]
  /** Unit state polling interval in milliseconds. Default 250. */
  readonly pollMs?: number
}

interface UnitState { readonly active: boolean; readonly invocation: string }

/** Linux systemd runtime. Each domain is one instance of a host-defined template
 * unit; the unit, not this process, applies the sandbox (user, mount namespace,
 * capabilities, resource limits) so the policy is reviewable host configuration.
 * The ingress reaches each runtime through its private Unix socket, as with Docker.
 * The generation lives in spec/<id>.env, which the unit reads as root before
 * dropping privileges and never mounts into the runtime, so a workload cannot
 * forge the ownership record that stop and recover verify. Stopping a unit
 * removes its whole cgroup, including setsid descendants.
 */
export class SystemdRuntimeProvider implements RuntimeProvider {
  private readonly unit: string
  private readonly systemctl: readonly string[]
  private readonly pollMs: number

  constructor(private readonly options: SystemdRuntimeOptions) {
    if (process.platform !== 'linux') throw new Error('Systemd provider requires Linux')
    this.unit = options.unit ?? 'dsh-domain'
    if (!/^[a-zA-Z0-9_.-]+$/.test(this.unit)) throw new TypeError('Invalid unit template name')
    this.systemctl = options.systemctl ?? ['systemctl']
    if (this.systemctl.length === 0) throw new TypeError('systemctl command must not be empty')
    this.pollMs = options.pollMs ?? 250
    if (!Number.isSafeInteger(this.pollMs) || this.pollMs <= 0 || this.pollMs > 10_000) throw new TypeError('Invalid unit polling interval')
  }

  private async run(args: string[]): Promise<string> {
    const [command, ...prefix] = this.systemctl
    const result = await exec(command!, [...prefix, ...args], { maxBuffer: 1_048_576, timeout: 30_000 })
    return result.stdout
  }

  private async state(name: string): Promise<UnitState> {
    const values = new Map<string, string>()
    for (const line of (await this.run(['show', name, '-p', 'ActiveState', '-p', 'InvocationID'])).split('\n')) {
      const at = line.indexOf('=')
      if (at > 0) values.set(line.slice(0, at), line.slice(at + 1).trim())
    }
    const active = values.get('ActiveState')
    if (active === undefined) throw new Error('systemctl show returned no ActiveState')
    // failed and inactive own no processes; every other state may.
    return { active: active !== 'inactive' && active !== 'failed', invocation: values.get('InvocationID') ?? '' }
  }

  private async location(spec: RuntimeSpec) {
    if (!/^[a-f0-9-]{36}$/.test(spec.domainId)) throw new TypeError('Invalid domain id')
    await mkdir(resolve(this.options.directory), { recursive: true, mode: 0o700 })
    const root = await realpath(this.options.directory)
    // Linux sockaddr_un reserves one of its 108 bytes for the terminator.
    if (Buffer.byteLength(join(root, 'control', spec.domainId, 'http.sock')) > 107) {
      throw new Error('Runtime directory is too long for a Linux Unix socket; use a shorter platform directory')
    }
    return {
      name: `${this.unit}@${spec.domainId}.service`,
      control: join(root, 'control', spec.domainId), data: join(root, 'data', spec.domainId),
      workspace: join(root, 'workspaces', spec.domainId), profile: join(root, 'profiles', spec.domainId),
      specDirectory: join(root, 'spec'), specFile: join(root, 'spec', `${spec.domainId}.env`),
    }
  }

  /** The platform-written ownership record, or undefined when absent. */
  private async record(file: string): Promise<Map<string, string> | undefined> {
    let text: string
    try { text = await readFile(file, 'utf8') }
    catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined; throw error }
    return new Map(text.split('\n').filter(line => line.includes('=')).map(line => [line.slice(0, line.indexOf('=')), line.slice(line.indexOf('=') + 1)]))
  }

  private async halt(name: string): Promise<void> {
    // systemctl stop blocks until the unit's cgroup is empty or its stop timeout escalates to SIGKILL.
    await this.run(['stop', name])
    if ((await this.state(name)).active) throw new Error('Runtime unit remains active after stop')
  }

  async recover(spec: RuntimeSpec): Promise<void> {
    const location = await this.location(spec)
    if ((await this.state(location.name)).active) {
      const record = await this.record(location.specFile)
      if (record?.get('DSH_DOMAIN_ID') !== spec.domainId || record.get('DSH_RUNTIME_GENERATION') !== String(spec.generation)) {
        throw new Error('Refusing to recover a foreign runtime generation')
      }
      await this.halt(location.name)
    }
    await rm(location.control, { recursive: true, force: true })
    await rm(location.specFile, { force: true })
  }

  acquire(spec: RuntimeSpec, signal: AbortSignal): RuntimeHandle {
    signal.throwIfAborted()
    const claim = randomUUID()
    let location: Awaited<ReturnType<SystemdRuntimeProvider['location']>> | undefined
    let transport: Awaited<ReturnType<typeof openPrivateSocket>> | undefined
    let attemptedStart = false
    let invocation = ''
    let stopping = false
    let stopTask: Promise<void> | undefined
    const cancelled = new AbortController()
    let exit!: () => void
    const exited = new Promise<void>(resolve => { exit = resolve })
    const check = () => { if (stopping || signal.aborted || cancelled.signal.aborted) throw new Error('Runtime start cancelled') }

    const acquisition = Promise.resolve().then(async () => {
      location = await this.location(spec)
      // No earlier generation is adopted by acquire. Recovery is a separate proof step.
      if ((await this.state(location.name)).active) throw new Error('Runtime already exists; verified recovery required')
      const profile = await lstat(join(location.profile, 'runtime.patch.json')).catch(() => undefined)
      if (!profile?.isFile()) throw new Error('Runtime profile is not provisioned at profiles/<domainId>/runtime.patch.json')
      for (const path of [location.data, location.control, location.workspace, location.specDirectory]) await mkdir(path, { recursive: true, mode: 0o700 })
      await rm(join(location.control, 'ready.json'), { force: true })
      await rm(join(location.control, 'http.sock'), { force: true })
      const temporary = `${location.specFile}.${claim}.tmp`
      await writeFile(temporary, [
        `DSH_DOMAIN_ID=${spec.domainId}`, `DSH_RUNTIME_GENERATION=${spec.generation}`,
        `DSH_RUNTIME_VERSION=${spec.version}`, `DSH_CLAIM=${claim}`, '',
      ].join('\n'), { mode: 0o600 })
      await rename(temporary, location.specFile)
      check()
      attemptedStart = true
      await this.run(['reset-failed', location.name]).catch(() => {
        // reset-failed exits non-zero for a unit with no failed state; start below is authoritative.
      })
      // A blocking start returns once the main process is forked; systemd keeps the
      // InvocationID after the run exits, so an immediate exit is still observed below.
      await this.run(['start', location.name])
      invocation = (await this.state(location.name)).invocation
      if (!invocation) throw new Error('Runtime unit reported no invocation after start')
      void (async () => {
        // The unit's InvocationID identifies this start; a different one means another run owns the name.
        // A transient systemctl failure is retried; persistent failure reports exit, and the next
        // acquire's active-unit check then forces verified recovery instead of a second writer.
        for (let failures = 0; failures < 3;) {
          let state: UnitState
          try { state = await this.state(location!.name); failures = 0 }
          catch { failures++; await delay(this.pollMs); continue }
          if (!state.active || state.invocation !== invocation) break
          await delay(this.pollMs)
        }
        cancelled.abort()
        exit()
      })()
    })

    const ready: Promise<RuntimeReady> = acquisition.then(async () => {
      for (;;) {
        if (stopping || signal.aborted) throw new Error('Runtime start cancelled')
        if (cancelled.signal.aborted) throw new Error('Runtime exited before readiness')
        let value: unknown
        try { value = await readRuntimeReady(join(location!.control, 'ready.json')) }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error }
        if (value !== undefined) {
          const candidate = value as RuntimeReady
          if (candidate.domainId !== spec.domainId || candidate.generation !== spec.generation || candidate.version !== spec.version
            || candidate.endpoint !== 'http://127.0.0.1:3081' || !candidate.authentication?.cookie || /[\r\n]/.test(candidate.authentication.cookie)) {
            throw new Error('Isolated runtime readiness mismatch')
          }
          transport = await openPrivateSocket(join(location!.control, 'http.sock'))
          return { ...spec, endpoint: candidate.endpoint, authentication: { cookie: candidate.authentication.cookie }, socketPath: transport.path }
        }
        await delay(25)
      }
    })
    void ready.catch(() => {})

    return { ready, exited, stop: () => {
      stopping = true
      if (stopTask) return stopTask
      stopTask = Promise.resolve().then(async () => {
        await acquisition.catch(() => {})
        await ready.catch(() => {})
        const errors: unknown[] = []
        const attempt = async (operation: () => Promise<void>) => { try { await operation() } catch (error) { errors.push(error) } }
        await attempt(async () => { if (transport) { await transport.file.close(); transport = undefined } })
        await attempt(async () => {
          if (!attemptedStart || !location) return
          const record = await this.record(location.specFile)
          // Only the run that wrote this claim may stop the unit; a newer acquire owns the name otherwise.
          if (record !== undefined && record.get('DSH_CLAIM') !== claim) throw new Error('Runtime ownership changed')
          await this.halt(location.name)
          await rm(location.control, { recursive: true, force: true })
          await rm(location.specFile, { force: true })
        })
        if (errors.length === 1) throw errors[0]
        if (errors.length) throw new AggregateError(errors, 'Runtime transport and unit cleanup failed')
        cancelled.abort()
        exit()
      }).catch(error => { stopTask = undefined; throw error })
      return stopTask
    } }
  }
}
