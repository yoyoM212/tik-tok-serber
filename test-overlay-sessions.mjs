import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {createServer} from 'node:http';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Server} from 'socket.io';
import {io as client} from 'socket.io-client';
import {installOverlaySessions} from './overlay-sessions.js';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const channel='a'.repeat(64),writeKey='b'.repeat(64),secret='test-only-shared-secret-never-deploy';
const roots=[],bridges=[],clients=[];
async function bridge(){
 const directory=mkdtempSync(join(tmpdir(),'loveligo-session-test-'));roots.push(directory);
 const http=createServer(),io=new Server(http),liveConnections=new Map();let starts=0,closed=0,grace;
 const roomOf=u=>'tiktok:'+u,roomSize=r=>io.sockets.adapter.rooms.get(r)?.size||0;
 const connectTikTok=async u=>{clearTimeout(grace);if(!liveConnections.has(u)){starts++;const adapter=new EventEmitter();adapter.online=true;adapter.isConnected=()=>adapter.online;for(const k of ['tiktok_like','tiktok_gift'])adapter.on(k,d=>io.to(roomOf(u)).emit(k,d));liveConnections.set(u,{adapter});}return 'https://local.test/avatar.png';};
 const scheduleDisconnect=u=>{clearTimeout(grace);grace=setTimeout(()=>{if(!roomSize(roomOf(u))&&!sessions.hasViewers(u)){closed++;liveConnections.delete(u);}},40);};
 const sessions=installOverlaySessions({io,connectTikTok,liveConnections,roomOf,roomSize,scheduleDisconnect,secret,directory});
 io.use((s,next)=>s.handshake.auth.owner||sessions.canRead(s.handshake.auth)?next():next(Error('AUTH_REQUIRED')));
 io.on('connection',s=>{s.on('join_game',async(d,ack)=>{await connectTikTok(d.username);s.currentJoinedUser=d.username;s.join(roomOf(d.username));ack({ok:true});});s.on('disconnect',()=>{if(s.currentJoinedUser&&!roomSize(roomOf(s.currentJoinedUser)))scheduleDisconnect(s.currentJoinedUser);});});
 await new Promise(r=>http.listen(0,'127.0.0.1',r));const url='http://127.0.0.1:'+http.address().port;
 const b={io,http,url,liveConnections,sessions,get starts(){return starts;},get closed(){return closed;},stop:()=>new Promise(r=>io.close(r))};bridges.push(b);return b;
}
async function socket(b,auth){const s=client(b.url,{auth,transports:['websocket'],reconnection:false});clients.push(s);await new Promise((r,j)=>{s.once('connect',r);s.once('connect_error',j);});return s;}
const emit=(s,k,d)=>new Promise((r,j)=>s.timeout(2500).emit(k,d,(e,v)=>e?j(e):r(v)));
try{
 const a=await bridge(),owner=await socket(a,{owner:true});
 assert.equal((await emit(owner,'join_game',{username:'streamer'})).ok,true);
 const opened=await emit(owner,'overlay_open',{channel,writeKey});assert(opened.ok);
 const obs1=await socket(a,{overlayChannel:channel}),obs2=await socket(a,{overlayChannel:channel});
 assert((await emit(obs1,'overlay_open',{channel})).ok);assert((await emit(obs2,'overlay_open',{channel})).ok);assert.equal(a.starts,1);
 let last;obs1.on('overlay_snapshot',p=>last=p);
 a.liveConnections.get('streamer').adapter.emit('tiktok_like',{liker:'X',nickname:'X',likeCount:1000,userTotalLikes:99999});
 a.liveConnections.get('streamer').adapter.emit('tiktok_gift',{gifter:'X',nickname:'X',eventId:'gift-1',diamondCount:10,repeatCount:3,totalDiamonds:30,isStreakable:true,isStreakEnd:false});
 a.liveConnections.get('streamer').adapter.emit('tiktok_gift',{gifter:'X',nickname:'X',eventId:'gift-2',diamondCount:10,repeatCount:3,totalDiamonds:30,isStreakable:true,isStreakEnd:true});
 a.liveConnections.get('streamer').adapter.emit('tiktok_gift',{gifter:'X',nickname:'X',eventId:'gift-2',totalDiamonds:30});
 await sleep(1100);assert.equal(last.snapshot.rankLists.likes[0].score,1000);assert.equal(last.snapshot.rankLists.gifts[0].score,30);assert(last.resume.signature);
 owner.disconnect();await sleep(80);assert.equal(a.closed,0);
 a.liveConnections.get('streamer').adapter.emit('tiktok_like',{liker:'X',likeCount:1});await sleep(1100);assert.equal(last.snapshot.rankLists.likes[0].score,1001);
 const upstream=a.liveConnections.get('streamer').adapter;upstream.online=false;upstream.emit('tiktok_disconnected',{temporary:true});assert.equal((await emit(obs1,'overlay_open',{channel})).error,'UPSTREAM_RECONNECTING');upstream.online=true;upstream.emit('tiktok_reconnected',{});
 // A second bridge imports a signed baseline once; extra sources do not add it again.
 const b=await bridge();assert(b.sessions.canRead({overlayChannel:channel,resume:last.resume}));
 assert(!b.sessions.canRead({overlayChannel:channel,resume:{...last.resume,body:last.resume.body.replace('1001','999999')}}));
 const migrated=await socket(b,{overlayChannel:channel,resume:last.resume});const restored=await emit(migrated,'overlay_open',{channel,resume:last.resume});assert(restored.ok);assert.equal(restored.snapshot.rankLists.likes[0].score,1001);
 const second=await socket(b,{overlayChannel:channel});const twice=await emit(second,'overlay_open',{channel,resume:last.resume});assert.equal(twice.snapshot.rankLists.likes[0].score,1001);assert.equal(b.starts,1);
 const next=new Promise(r=>migrated.once('overlay_snapshot',r));b.liveConnections.get('streamer').adapter.emit('tiktok_like',{liker:'X',likeCount:1,userTotalLikes:1});const recovered=await next;assert.equal(recovered.snapshot.rankLists.likes[0].score,1002);
 const back=await emit(obs1,'overlay_open',{channel,resume:recovered.resume});assert.equal(back.snapshot.rankLists.likes[0].score,1002);
 const denied=await emit(migrated,'overlay_open',{channel,writeKey:'c'.repeat(64)});assert.equal(denied.error,'OWNER_MISMATCH');
 assert.equal((await emit(migrated,'overlay_rank_command',{reset:'likes'})).error,'OWNER_REQUIRED');
 const manager=await socket(b,{owner:true});assert((await emit(manager,'overlay_open',{channel,writeKey})).ok);
 assert((await emit(manager,'overlay_rank_command',{reset:'likes'})).ok);
 const reset=await emit(manager,'overlay_open',{channel,writeKey,resume:recovered.resume});assert.equal(reset.snapshot.rankLists.likes.length,0);manager.disconnect();
 obs1.disconnect();obs2.disconnect();await sleep(80);assert.equal(a.closed,1);
 migrated.disconnect();second.disconnect();await sleep(80);assert.equal(b.closed,1);
 console.log('PASS real local Socket.IO: shared upstream, dashboard closed/OBS continues, 1000→1001→1002 failover, identical baseline on extra sources, gift streak/dedup, forged snapshot rejected, owner protection, final disconnect grace. No real TikTok broadcast.');
}finally{for(const s of clients)s.disconnect();for(const b of bridges)await b.stop();for(const directory of roots)rmSync(directory,{recursive:true,force:true});}
