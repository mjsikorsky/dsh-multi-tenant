import { mkdtemp } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { DatabaseSync } from 'node:sqlite'
import { afterEach, expect, it } from 'vitest'
import { SQLiteDomainRepository } from '../../src/domain/sqlite.ts'
import { DomainRuntimeCoordinator } from '../../src/runtime/coordinator.ts'
import type { DomainScope } from '../../src/domain/repository.ts'
import type { RuntimeProvider, RuntimeSpec } from '../../src/runtime/provider.ts'
const cleanups: (() => unknown)[] = []
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup() })
const scope = (tenantId = 'acme', workspaceId = 'room'): DomainScope => ({ kind: 'workspace', tenantId, workspaceId })
async function directory() { return mkdtemp(join(tmpdir(), 'dsh-workspace-scope-')) }
const oldSchema = `CREATE TABLE domains_v1 (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, principal_id TEXT NOT NULL, desired TEXT NOT NULL,
 revision INTEGER NOT NULL, generation INTEGER NOT NULL, state TEXT NOT NULL, unresolved INTEGER NOT NULL,
 UNIQUE (tenant_id,principal_id)) STRICT;`
it('scoped UUIDs remain distinct from personal identifiers, delimiter collisions and other tenants', async () => {
 const root = await directory(); const repo = new SQLiteDomainRepository(root); cleanups.push(() => repo.close())
 const shared = repo.resolve(scope()); expect(repo.resolve(scope()).id).toBe(shared.id)
 expect(shared.scope).toEqual(scope()); expect('owner' in shared).toBe(false)
 expect(repo.resolve({ tenantId: 'acme', principalId: 'room' }).id).not.toBe(shared.id)
 expect(repo.resolve(scope('other')).id).not.toBe(shared.id)
 expect(repo.resolve(scope('a:b','c')).id).not.toBe(repo.resolve(scope('a','b:c')).id)
 repo.close(); const reopened = new SQLiteDomainRepository(root); cleanups.push(() => reopened.close())
 expect(reopened.resolve(scope()).id).toBe(shared.id)
})
it('migrates personal v1 UUIDs and retained quarantine atomically without claiming shared ownership', async () => {
 const root = await directory();const db = new DatabaseSync(join(root,'domains.sqlite'))
 db.exec(oldSchema);db.prepare('INSERT INTO domains_v1 VALUES (?,?,?,?,?,?,?,?)').run('old-uuid','acme','alice','suspended',3,9,'ready',1);db.close()
 const repo = new SQLiteDomainRepository(root);cleanups.push(() => repo.close())
 const migrated = repo.resolve({ tenantId:'acme',principalId:'alice' })
 expect(migrated).toMatchObject({ id:'old-uuid',generation:9,revision:3,desired:'suspended',state:'failed',unresolved:true,
  owner:{tenantId:'acme',principalId:'alice'},scope:{kind:'personal',tenantId:'acme',principalId:'alice'} })
 expect(repo.resolve(scope('acme','alice')).id).not.toBe('old-uuid')
 repo.close();const after = new DatabaseSync(join(root,'domains.sqlite'));cleanups.push(() => after.close())
 expect(after.prepare("SELECT type FROM sqlite_master WHERE name='domains_v1'").get()?.type).toBe('view')
 expect(() => after.exec("UPDATE domains_v1 SET state='failed' WHERE unresolved=1")).toThrow()
 expect(after.prepare('PRAGMA user_version').get()?.user_version).toBe(2)
})
it('refuses a conflicting migration without deleting or replacing either owner record', async () => {
 const root = await directory();const first = new SQLiteDomainRepository(root)
 const selected = first.resolve({tenantId:'acme',principalId:'alice'});first.close()
 const db = new DatabaseSync(join(root,'domains.sqlite'));db.exec('DROP VIEW domains_v1');db.exec(oldSchema)
 db.prepare('INSERT INTO domains_v1 VALUES (?,?,?,?,?,?,?,?)').run('conflicting-old','acme','alice','enabled',0,0,'stopped',0);db.close()
 expect(() => new SQLiteDomainRepository(root)).toThrow('Cannot acquire')
 const after = new DatabaseSync(join(root,'domains.sqlite'));cleanups.push(() => after.close())
 expect(after.prepare('SELECT id FROM domains_v1').get()?.id).toBe('conflicting-old')
 expect(after.prepare('SELECT id FROM domains_v2').get()?.id).toBe(selected.id)
})
it('rejects mixed scope shapes, missing tags and future database schema before allocation', async () => {
 const root = await directory();const repo = new SQLiteDomainRepository(root)
 for(const value of [scope('acme',' '), {...scope(),principalId:'alice'}, {tenantId:'acme',workspaceId:'room'}, {...scope(),kind:'unknown'}]) {
  expect(() => repo.resolve(value as DomainScope)).toThrow()
 }
 repo.close();const db = new DatabaseSync(join(root,'domains.sqlite'));db.exec('PRAGMA user_version=3');db.close()
 expect(() => new SQLiteDomainRepository(root)).toThrow('Cannot acquire')
 const after = new DatabaseSync(join(root,'domains.sqlite'));cleanups.push(() => after.close())
 expect(after.prepare('PRAGMA user_version').get()?.user_version).toBe(3)
})
it('shares one native generation for one workspace and revokes only that native domain', async () => {
 const root=await directory();const repository=new SQLiteDomainRepository(root);const starts:RuntimeSpec[]=[]
 const provider:RuntimeProvider={acquire(spec){starts.push(spec);const exited=Promise.withResolvers<void>();return{
  ready:Promise.resolve({...spec,endpoint:'http://127.0.0.1:3081'}),exited:exited.promise,async stop(){exited.resolve()},
 }}}
 const runtime=new DomainRuntimeCoordinator(repository,provider,'native');cleanups.push(() => runtime.close())
 const [alice,bob]=await Promise.all([runtime.ensure(scope()),runtime.ensure(scope())]);expect(alice).toBe(bob);expect(starts).toHaveLength(1)
 const other=await runtime.ensure(scope('acme','other'));expect(other.domainId).not.toBe(alice.domainId)
 await runtime.setDesired(alice.domainId,'revoked');expect(alice.signal.aborted).toBe(true);expect(other.signal.aborted).toBe(false)
 await expect(runtime.ensure(scope())).rejects.toThrow('not enabled')
})

it('current only reads an already-ready exact generation and rechecks revocation across its await', async () => {
 const root=await directory();const repository=new SQLiteDomainRepository(root);let starts=0
 const ready=Promise.withResolvers<{domainId:string;generation:number;version:string;endpoint:string}>()
 const exited=Promise.withResolvers<void>()
 const runtime=new DomainRuntimeCoordinator(repository,{acquire(){starts++;return{ready:ready.promise,exited:exited.promise,async stop(){exited.resolve()}}}},'native')
 cleanups.push(()=>runtime.close())
 const record=repository.resolve(scope())
 expect(await runtime.current(record.id,1)).toBeUndefined();expect(starts).toBe(0)
 const starting=runtime.ensure(scope());await Promise.resolve()
 expect(await runtime.current(record.id,1)).toBeUndefined();expect(starts).toBe(1)
 ready.resolve({domainId:record.id,generation:1,version:'native',endpoint:'http://127.0.0.1:3081'})
 const admission=await starting
 expect(await runtime.current(record.id,1)).toBe(admission)
 expect(await runtime.current(record.id,2)).toBeUndefined()
 const reading=runtime.current(record.id,1)
 const stopping=runtime.setDesired(record.id,'revoked')
 expect(await reading).toBeUndefined();await stopping
 expect(await runtime.current(record.id,1)).toBeUndefined();expect(starts).toBe(1)
})
