// Stand-in for systemctl driving one template unit. Unit state is a JSON file per
// unit under the state directory; "start" spawns the runtime fixture detached with
// the environment a dsh-domain@ unit would give it (spec EnvironmentFile + control dir).
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { existsSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const [stateDirectory, root, mode, verb, ...rest] = process.argv.slice(2)
const name = rest.filter(value => !value.startsWith('-') && value.endsWith('.service'))[0]
const file = join(stateDirectory, `${name}.json`)
const alive = pid => { try { process.kill(pid, 0); return true } catch { return false } }
const recorded = () => existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : undefined
const current = () => {
  const unit = recorded()
  return unit && alive(unit.pid) ? unit : undefined
}

if (mode === 'show-fails' && verb === 'show' && existsSync(join(stateDirectory, 'show-fails'))) process.exit(1)
if (verb === 'show') {
  // Like systemd, the last InvocationID survives the run's exit until the unit is stopped.
  const unit = recorded()
  process.stdout.write(`ActiveState=${current() ? 'active' : 'inactive'}\nInvocationID=${unit ? unit.invocation : ''}\n`)
} else if (verb === 'reset-failed') {
  process.exit(1)
} else if (verb === 'start') {
  if (current()) process.exit(0)
  const domainId = name.slice(name.indexOf('@') + 1, -'.service'.length)
  const spec = Object.fromEntries(readFileSync(join(root, 'spec', `${domainId}.env`), 'utf8')
    .split('\n').filter(Boolean).map(line => [line.slice(0, line.indexOf('=')), line.slice(line.indexOf('=') + 1)]))
  const child = spawn(process.execPath, [fileURLToPath(new URL('./systemd-runtime.fixture.mjs', import.meta.url))], {
    detached: true, stdio: 'ignore',
    env: { ...spec, DSH_CONTROL_DIR: join(root, 'control', domainId), PROBE_MODE: mode },
  })
  child.unref()
  writeFileSync(file, JSON.stringify({ pid: child.pid, invocation: randomUUID().replaceAll('-', '') }))
} else if (verb === 'stop') {
  const unit = current()
  if (unit) {
    // Mirrors KillMode=control-group with a stop timeout: SIGTERM the group, then SIGKILL.
    try { process.kill(-unit.pid, 'SIGTERM') } catch {}
    const until = Date.now() + 1_000
    while (alive(unit.pid) && Date.now() < until) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10)
    if (alive(unit.pid)) { try { process.kill(-unit.pid, 'SIGKILL') } catch {} }
    while (alive(unit.pid)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10)
  }
  rmSync(file, { force: true })
} else {
  process.stderr.write(`unsupported systemctl verb: ${verb}\n`)
  process.exit(2)
}
