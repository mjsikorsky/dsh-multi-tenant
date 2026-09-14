#!/usr/bin/env node
import { execFileSync } from 'node:child_process'
import { readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { installArtifact } from './installed-package.mjs'
const installed = installArtifact(process.argv[2] ?? '--local')
try {
  const guide = readFileSync(join(installed.packageDirectory, 'AI.md'), 'utf8')
  if (!guide.includes('runtime-manifest.json')) throw new Error('Installed package is missing its AI operating guide')
  execFileSync('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', '--save-dev', 'typescript@6.0.3', '@types/node@22.20.0'], { cwd: installed.directory, stdio: 'pipe' })
  writeFileSync(join(installed.directory, 'contract.ts'), `
    import { DomainRuntimeCoordinator, SQLiteDomainRepository, createDomainIngress,
      DSH_RUNTIME_VERSION, AUTHENTICATED_CONTEXT_HEADER, openPrivateSocket, type DomainScope, type AuthenticatedSession, type DomainAuthenticator, type RuntimeProvider } from 'dsh-multi-tenant'
    const contextHeader: 'x-dsh-authenticated-context' = AUTHENTICATED_CONTEXT_HEADER
    const context: AuthenticatedSession = { owner: { tenantId: 'tenant', principalId: 'person' }, signal: new AbortController().signal, authenticatedContext: 'signed-proof' }
    void contextHeader; void context
    declare const privateSocket: Awaited<ReturnType<typeof openPrivateSocket>>
    const privateSocketPath: string = privateSocket.path
    const closePrivateSocket: () => Promise<void> = () => privateSocket.file.close()
    void privateSocketPath; void closePrivateSocket
    declare const authentication: DomainAuthenticator
    declare const provider: RuntimeProvider
    const repository = new SQLiteDomainRepository('/platform/directory')
    const workspace: DomainScope = { kind: 'workspace', tenantId: 'tenant', workspaceId: 'room' }
    const shared = repository.resolve(workspace)
    const selected: DomainScope = shared.scope
    const personal = repository.resolve({ tenantId: 'tenant', principalId: 'person' })
    const actor: string = personal.owner.principalId
    void selected; void actor
    const collaborator: AuthenticatedSession = { ...context, scope: workspace }
    void collaborator
    const runtime = new DomainRuntimeCoordinator(repository, provider, DSH_RUNTIME_VERSION)
    void runtime.ensure(workspace)
    void runtime.current(shared.id, shared.generation)
    createDomainIngress({ authenticator: authentication, runtime, originFor: () => 'https://alice.example' })
    // @ts-expect-error No per-root authorization facade remains.
    runtime.revokeRoot('session')
  `)
  execFileSync(process.execPath, ['node_modules/typescript/bin/tsc', '--noEmit', '--strict', '--types', 'node', '--module', 'NodeNext', '--target', 'ES2024', 'contract.ts'], { cwd: installed.directory, stdio: 'inherit' })
  execFileSync(process.execPath, ['--input-type=module', '-e', "import { openPrivateSocket } from 'dsh-multi-tenant'; if (typeof openPrivateSocket !== 'function') throw new Error('Missing installed private socket SDK export')"], { cwd: installed.directory, stdio: 'inherit' })
  execFileSync(process.execPath, [join(installed.packageDirectory, 'examples/native-domains/smoke.mjs')], { cwd: installed.directory, stdio: 'inherit' })
  console.log('Installed domain lifecycle, ingress, native asset and declaration contract passed')
} finally { installed.close() }
