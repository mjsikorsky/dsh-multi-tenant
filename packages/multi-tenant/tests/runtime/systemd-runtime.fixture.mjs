// Stand-in for the native runtime inside a dsh-domain@ unit: serves HTTP on the
// private control socket, then publishes ready.json the way runtime-control.mjs does.
import { createServer } from 'node:http'
import { rename, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

const control = process.env.DSH_CONTROL_DIR
const mode = process.env.PROBE_MODE
const server = createServer((_request, response) => response.end(process.env.DSH_DOMAIN_ID))
server.listen(join(control, 'http.sock'), async () => {
  if (mode === 'never-ready') return
  if (mode === 'exit-before-ready') process.exit(3)
  const ready = {
    type: 'runtime-ready', domainId: process.env.DSH_DOMAIN_ID,
    generation: Number(process.env.DSH_RUNTIME_GENERATION) + (mode === 'wrong-generation' ? 1 : 0),
    version: process.env.DSH_RUNTIME_VERSION, endpoint: 'http://127.0.0.1:3081',
    authentication: { cookie: 'session=probe' },
  }
  await writeFile(join(control, 'ready.tmp'), JSON.stringify(ready), { mode: 0o600 })
  await rename(join(control, 'ready.tmp'), join(control, 'ready.json'))
  if (mode === 'crash-after-ready') setTimeout(() => process.exit(4), 300)
})
