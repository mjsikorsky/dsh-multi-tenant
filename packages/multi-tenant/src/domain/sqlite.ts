import { randomUUID } from 'node:crypto'
import { mkdirSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { normalizeDomainScope, type DomainDesiredState, type DomainOwner, type DomainScope, type DomainRecord, type PersonalDomainRecord, type DomainRepository } from './repository.ts'

/** Single-active coordinator, local storage only; directory must be platform-owned.
 * A separate SQLite exclusive transaction is the OS-released coordinator lock.
 * It is NOT a runtime/storage fence. Unclean domain leases remain quarantined.
 */
export class SQLiteDomainRepository implements DomainRepository {
  private readonly guard: DatabaseSync
  private readonly database: DatabaseSync
  private closed = false

  constructor(directory: string) {
    const root = resolve(directory)
    mkdirSync(root, { recursive: true, mode: 0o700 })
    const guard = new DatabaseSync(join(root, 'coordinator.sqlite'))
    let database: DatabaseSync | undefined
    let migrating = false
    try {
      guard.exec('PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;')
      database = new DatabaseSync(join(root, 'domains.sqlite'))
      const schema = database.prepare('PRAGMA user_version').get()
      if (Number(schema?.user_version) > 2) throw new Error('Unsupported future domain schema')
      database.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; BEGIN IMMEDIATE;')
      migrating = true
      // A v1 personal directory is migrated atomically. Its UUID, generation and
      // unresolved ownership are facts, never reassigned to a workspace scope.
      database.exec(`
        CREATE TABLE IF NOT EXISTS domains_v2 (
          id TEXT PRIMARY KEY,
          kind TEXT NOT NULL CHECK (kind IN ('personal','workspace')),
          tenant_id TEXT NOT NULL,
          principal_id TEXT,
          workspace_id TEXT,
          desired TEXT NOT NULL CHECK (desired IN ('enabled','suspended','revoked')),
          revision INTEGER NOT NULL CHECK (revision >= 0),
          generation INTEGER NOT NULL CHECK (generation >= 0),
          state TEXT NOT NULL CHECK (state IN ('stopped','starting','ready','stopping','failed')),
          unresolved INTEGER NOT NULL CHECK (unresolved IN (0,1)),
          CHECK ((kind = 'personal' AND principal_id IS NOT NULL AND workspace_id IS NULL)
            OR (kind = 'workspace' AND workspace_id IS NOT NULL AND principal_id IS NULL))
        ) STRICT;
        CREATE UNIQUE INDEX IF NOT EXISTS domains_v2_personal ON domains_v2 (tenant_id, principal_id) WHERE kind = 'personal';
        CREATE UNIQUE INDEX IF NOT EXISTS domains_v2_workspace ON domains_v2 (tenant_id, workspace_id) WHERE kind = 'workspace';
      `)
      if (database.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'domains_v1'").get()) {
        // Plain INSERT deliberately rejects any pre-existing conflicting scope/UUID.
        // Never replace a record, merge two domains, or guess migration completion.
        database.exec(`INSERT INTO domains_v2
          SELECT id, 'personal', tenant_id, principal_id, NULL, desired, revision, generation, state, unresolved FROM domains_v1;
          DROP TABLE domains_v1;`)
      }
      // A read-only compatibility view makes an older v1 coordinator fail its
      // startup UPDATE instead of silently creating a second writable directory.
      database.exec(`CREATE VIEW IF NOT EXISTS domains_v1 AS
        SELECT id, tenant_id, principal_id, desired, revision, generation, state, unresolved
        FROM domains_v2 WHERE kind = 'personal';`)
      database.exec("UPDATE domains_v2 SET state = 'failed' WHERE unresolved = 1; PRAGMA user_version = 2; COMMIT;")
      migrating = false
    } catch (error) {
      const errors = [error]
      if (migrating) { try { database?.exec('ROLLBACK') } catch (cleanup) { errors.push(cleanup) } }
      try { database?.close() } catch (cleanup) { errors.push(cleanup) }
      try { guard.close() } catch (cleanup) { errors.push(cleanup) }
      throw new AggregateError(errors, 'Cannot acquire the domain directory coordinator')
    }
    this.guard = guard
    this.database = database
  }

  resolve(owner: DomainOwner): PersonalDomainRecord
  resolve(scope: DomainScope): DomainRecord
  resolve(input: DomainOwner | DomainScope): DomainRecord
  resolve(input: DomainOwner | DomainScope): DomainRecord {
    const scope = normalizeDomainScope(input)
    const personal = scope.kind === 'personal'
    this.database.prepare(`INSERT INTO domains_v2 VALUES (?, ?, ?, ?, ?, 'enabled', 0, 0, 'stopped', 0)
      ON CONFLICT DO NOTHING`).run(randomUUID(), scope.kind, scope.tenantId,
      personal ? scope.principalId : null, personal ? null : scope.workspaceId)
    const row = this.database.prepare(`SELECT id FROM domains_v2 WHERE kind = ? AND tenant_id = ?
      AND ${personal ? 'principal_id' : 'workspace_id'} = ?`)
      .get(scope.kind, scope.tenantId, personal ? scope.principalId : scope.workspaceId)
    if (!row) throw new Error('Domain scope could not be resolved')
    return this.get(String(row.id))
  }

  get(id: string): DomainRecord {
    const row = this.database.prepare('SELECT * FROM domains_v2 WHERE id = ?').get(id)
    if (!row) throw new Error('Unknown domain')
    const scope = normalizeDomainScope(row.kind === 'personal'
      ? { kind: 'personal', tenantId: String(row.tenant_id), principalId: String(row.principal_id) }
      : { kind: 'workspace', tenantId: String(row.tenant_id), workspaceId: String(row.workspace_id) })
    const placement = scope.kind === 'personal'
      ? { scope, owner: Object.freeze({ tenantId: scope.tenantId, principalId: scope.principalId }) }
      : { scope }
    return Object.freeze({
      id: String(row.id),
      ...placement,
      desired: row.desired as DomainRecord['desired'],
      state: row.state as DomainRecord['state'],
      revision: Number(row.revision),
      generation: Number(row.generation),
      unresolved: row.unresolved === 1,
    })
  }

  begin(id: string): DomainRecord {
    const result = this.database.prepare(`UPDATE domains_v2
      SET generation = generation + 1, state = 'starting', unresolved = 1
      WHERE id = ? AND desired = 'enabled' AND unresolved = 0
        AND state IN ('stopped','failed') AND generation < 9007199254740991`).run(id)
    if (result.changes !== 1) throw new Error('Domain disabled, busy, or awaiting verified runtime recovery')
    return this.get(id)
  }

  ready(id: string, generation: number): void {
    const result = this.database.prepare(`UPDATE domains_v2 SET state = 'ready'
      WHERE id = ? AND generation = ? AND state = 'starting' AND desired = 'enabled'`).run(id, generation)
    if (result.changes !== 1) throw new Error('Stale domain readiness')
  }

  stopping(id: string, generation: number): void {
    this.database.prepare(`UPDATE domains_v2 SET state = 'stopping'
      WHERE id = ? AND generation = ? AND unresolved = 1`).run(id, generation)
  }

  finish(id: string, generation: number, released: boolean, failed: boolean): void {
    const result = this.database.prepare(`UPDATE domains_v2 SET state = ?, unresolved = ?
      WHERE id = ? AND generation = ?`).run(failed || !released ? 'failed' : 'stopped', released ? 0 : 1, id, generation)
    if (result.changes !== 1) throw new Error('Stale domain disposal')
  }

  setDesired(id: string, desired: DomainDesiredState): DomainRecord {
    const current = this.get(id)
    if (current.desired === desired) return current
    if (current.desired === 'revoked') throw new Error('Revoked domains cannot be re-enabled')
    const result = this.database.prepare(`UPDATE domains_v2 SET desired = ?, revision = revision + 1
      WHERE id = ? AND revision < 9007199254740991`).run(desired, id)
    if (result.changes !== 1) throw new Error('Cannot update domain policy')
    return this.get(id)
  }

  close(): void {
    if (this.closed) return
    this.closed = true
    const errors: unknown[] = []
    try { this.database.close() } catch (error) { errors.push(error) }
    try { this.guard.close() } catch (error) { errors.push(error) }
    if (errors.length) throw new AggregateError(errors, 'Domain directory close failed')
  }
}
