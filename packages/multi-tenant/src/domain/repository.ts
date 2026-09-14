/** An authenticated person. This identity is never rewritten to impersonate a workspace. */
export interface DomainOwner {
  readonly tenantId: string
  readonly principalId: string
}

/** Trusted runtime placement, independent of the person making a request. */
export type DomainScope =
  | { readonly kind: 'personal'; readonly tenantId: string; readonly principalId: string }
  | { readonly kind: 'workspace'; readonly tenantId: string; readonly workspaceId: string }

/** Snapshot and validate the complete tagged selector. Untagged owners preserve the personal API. */
export function normalizeDomainScope(value: DomainOwner | DomainScope): DomainScope {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TypeError('Invalid domain scope')
  const input = { ...value } as Record<string, unknown>
  const kind = 'kind' in input ? input.kind : 'personal'
  if (kind !== 'personal' && kind !== 'workspace') throw new TypeError('Invalid domain scope kind')
  const subject = kind === 'personal' ? 'principalId' : 'workspaceId'
  const keys = 'kind' in input ? ['kind', 'tenantId', subject] : ['tenantId', subject]
  if (Object.keys(input).length !== keys.length || Object.keys(input).some(key => !keys.includes(key))) {
    throw new TypeError('Domain scope contains unrelated identity fields')
  }
  for (const field of ['tenantId', subject]) {
    const item = input[field]
    if (typeof item !== 'string' || !item.trim() || item !== item.trim()) {
      throw new TypeError('Domain scope fields must be non-empty trimmed strings')
    }
  }
  return Object.freeze(kind === 'personal'
    ? { kind, tenantId: input.tenantId as string, principalId: input.principalId as string }
    : { kind, tenantId: input.tenantId as string, workspaceId: input.workspaceId as string })
}

export type DomainState = 'stopped' | 'starting' | 'ready' | 'stopping' | 'failed'
export type DomainDesiredState = 'enabled' | 'suspended' | 'revoked'
interface DomainFacts {
  readonly id: string
  readonly desired: DomainDesiredState
  readonly revision: number
  readonly generation: number
  readonly state: DomainState
  /** True until the provider positively confirms disposal, including failed starts. */
  readonly unresolved: boolean
}
export type PersonalDomainRecord = DomainFacts & {
  readonly scope: Extract<DomainScope, { kind: 'personal' }>
  /** Legacy personal-owner projection; absent on shared workspace records. */
  readonly owner: DomainOwner
}
export type WorkspaceDomainRecord = DomainFacts & {
  readonly scope: Extract<DomainScope, { kind: 'workspace' }>
  readonly owner?: never
}
export type DomainRecord = PersonalDomainRecord | WorkspaceDomainRecord

export interface DomainRepository {
  resolve(owner: DomainOwner): PersonalDomainRecord
  resolve(scope: DomainScope): DomainRecord
  resolve(input: DomainOwner | DomainScope): DomainRecord
  get(id: string): DomainRecord
  begin(id: string): DomainRecord
  ready(id: string, generation: number): void
  stopping(id: string, generation: number): void
  finish(id: string, generation: number, released: boolean, failed: boolean): void
  setDesired(id: string, desired: DomainDesiredState): DomainRecord
  close(): void
}
