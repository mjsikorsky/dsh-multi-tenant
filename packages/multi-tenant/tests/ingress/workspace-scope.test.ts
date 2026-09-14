import {createServer, type Server} from 'node:http'
import {once} from 'node:events'
import type {AddressInfo} from 'node:net'
import {mkdtemp} from 'node:fs/promises'
import {tmpdir} from 'node:os'
import {join} from 'node:path'
import {WebSocket,WebSocketServer} from 'ws'
import {afterEach,expect,it} from 'vitest'
import {createDomainIngress} from '../../src/ingress/server.ts'
import {SQLiteDomainRepository} from '../../src/domain/sqlite.ts'
import {DomainRuntimeCoordinator} from '../../src/runtime/coordinator.ts'
import type {DomainScope} from '../../src/domain/repository.ts'
const cleanups:(()=>unknown)[]=[]
afterEach(async()=>{for(const close of cleanups.splice(0).reverse())await close()})
async function listen(server:Server){server.listen(0,'127.0.0.1');await once(server,'listening');return `http://127.0.0.1:${(server.address() as AddressInfo).port}`}
async function setup(){
 const native=createServer((request,response)=>{response.end(JSON.stringify({actor:request.headers['x-dsh-authenticated-context'],cookie:request.headers.cookie,url:request.url}))})
 const socketServer=new WebSocketServer({server:native})
 socketServer.on('connection',(socket,request)=>socket.on('message',bytes=>socket.send(`${String(request.headers['x-dsh-authenticated-context'])}:${bytes.toString()}`)))
 const endpoint=await listen(native)
 cleanups.push(async()=>{for(const socket of socketServer.clients)socket.terminate();socketServer.close();native.closeAllConnections();await new Promise<void>(resolve=>native.close(()=>resolve()))})
 const repository=new SQLiteDomainRepository(await mkdtemp(join(tmpdir(),'dsh-shared-ingress-')))
 let starts=0;const runtime=new DomainRuntimeCoordinator(repository,{acquire(spec){starts++;const exit=Promise.withResolvers<void>();return{
  ready:Promise.resolve({...spec,endpoint,authentication:{cookie:'native=private'}}),exited:exit.promise,async stop(){exit.resolve()},
 }}},'native');cleanups.push(()=>runtime.close())
 let runtimeGeneration:number|undefined;let origin='';let allowBob=true;let scope:DomainScope={kind:'workspace',tenantId:'acme',workspaceId:'room'}
 const aliceSignal=new AbortController();const bobSignal=new AbortController();const owners:string[]=[]
 const ingress=createDomainIngress({runtime,authenticator:{async authenticate(request){
  const actor=request.headers.cookie;if(actor!=='alice'&&actor!=='bob'||actor==='bob'&&!allowBob)return
  return{owner:{tenantId:'acme',principalId:actor},scope,...(runtimeGeneration===undefined?{}:{runtimeGeneration}),signal:(actor==='alice'?aliceSignal:bobSignal).signal,authenticatedContext:actor}
 }},originFor(owner,selected){owners.push(owner.principalId);expect(selected.tenantId).toBe('acme');return origin}})
 origin=await listen(ingress.server);cleanups.push(()=>ingress.close())
 return{origin,runtime,repository,owners,get starts(){return starts},setGeneration(value:number){runtimeGeneration=value},denyBob(){allowBob=false;bobSignal.abort()},setScope(value:DomainScope){scope=value}}
}
it('two independently authenticated actors reach one native workspace generation with distinct native actor contexts',async()=>{
 const t=await setup()
 for(const actor of ['alice','bob']){
  const response=await fetch(t.origin+'/api/native?x=1',{headers:{cookie:actor,origin:t.origin}})
  expect(response.status).toBe(200);expect(await response.json()).toEqual({actor,cookie:'native=private',url:'/api/native?x=1'})
 }
 expect(t.starts).toBe(1);expect(t.owners).toEqual(['alice','bob'])
 const record=t.repository.resolve({kind:'workspace',tenantId:'acme',workspaceId:'room'});expect('owner' in record).toBe(false)
})
it('shared native WebSocket keeps actor context and actor revocation does not stop collaborators runtime',async()=>{
 const t=await setup();const connect=async(actor:string)=>{
  const socket=new WebSocket(t.origin.replace('http:','ws:')+'/api/remote.mux',{headers:{cookie:actor,origin:t.origin}})
  cleanups.push(()=>socket.terminate());await once(socket,'open');return socket
 }
 const alice=await connect('alice');const bob=await connect('bob');expect(t.starts).toBe(1)
 alice.send('a');expect(String((await once(alice,'message'))[0])).toBe('alice:a')
 bob.send('b');expect(String((await once(bob,'message'))[0])).toBe('bob:b')
 const closed=once(bob,'close');t.denyBob();await closed
 alice.send('still-live');expect(String((await once(alice,'message'))[0])).toBe('alice:still-live')
 expect((await fetch(t.origin,{headers:{cookie:'bob'}})).status).toBe(401)
 expect((await fetch(t.origin,{headers:{cookie:'alice'}})).status).toBe(200);expect(t.starts).toBe(1)
})
it('cross-tenant and another personal placement are denied before runtime allocation',async()=>{
 const t=await setup();t.setScope({kind:'workspace',tenantId:'foreign',workspaceId:'room'})
 expect((await fetch(t.origin,{headers:{cookie:'alice'}})).status).toBe(403)
 t.setScope({kind:'personal',tenantId:'acme',principalId:'bob'})
 expect((await fetch(t.origin,{headers:{cookie:'alice'}})).status).toBe(403);expect(t.starts).toBe(0)
})
it('snapshots actor and scope before origin callback can mutate authenticator objects',async()=>{
 const signal=new AbortController();const owner={tenantId:'acme',principalId:'alice'}
 const scope={kind:'workspace' as const,tenantId:'acme',workspaceId:'room'};let ensured:unknown;let origin=''
 const ingress=createDomainIngress({authenticator:{async authenticate(){return{owner,scope,signal:signal.signal}}},
  originFor(actor,selected){expect(Object.isFrozen(actor)).toBe(true);expect(Object.isFrozen(selected)).toBe(true);owner.principalId='bob';scope.workspaceId='other';return origin},
  runtime:{async ensure(selected){ensured=selected;throw new Error('Stop after observed native selector')}}})
 origin=await listen(ingress.server);cleanups.push(()=>ingress.close())
 expect((await fetch(origin)).status).toBe(503)
 expect(ensured).toEqual({kind:'workspace',tenantId:'acme',workspaceId:'room'})
})

it('a signed generation cannot route HTTP or WebSocket into a newly started native generation',async()=>{
 const t=await setup();t.setGeneration(1)
 expect((await fetch(t.origin,{headers:{cookie:'alice'}})).status).toBe(200)
 const record=t.repository.resolve({kind:'workspace',tenantId:'acme',workspaceId:'room'})
 await t.runtime.stop(record.id)
 expect((await fetch(t.origin,{headers:{cookie:'alice'}})).status).toBe(403)
 const socket=new WebSocket(t.origin.replace('http:','ws:')+'/api/remote.mux',{headers:{cookie:'alice',origin:t.origin}})
 cleanups.push(()=>socket.terminate())
 const status=await new Promise<number>((resolve,reject)=>{
  socket.on('error',()=>{})
  socket.once('unexpected-response',(_request,response)=>{response.resume();resolve(response.statusCode!)})
  socket.once('open',()=>reject(new Error('Stale generation admitted WebSocket')))
 })
 expect(status).toBe(403);expect(t.starts).toBe(2)
 t.setGeneration(2)
 expect((await fetch(t.origin,{headers:{cookie:'alice'}})).status).toBe(200)
})
