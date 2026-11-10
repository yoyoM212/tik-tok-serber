import {createHmac, createHash, timingSafeEqual, randomUUID} from 'node:crypto';
import {mkdirSync, readFileSync, writeFileSync, renameSync, readdirSync, statSync, unlinkSync} from 'node:fs';
import {resolve, join} from 'node:path';

const token = value => /^[a-f0-9]{64}$/.test(String(value || ''));
const hash = value => createHash('sha256').update(value).digest('hex');
const positive = value => Math.max(0, Math.min(1e9, Number(value) || 0));
const equal = (a,b) => typeof a==='string' && typeof b==='string' && a.length===b.length && timingSafeEqual(Buffer.from(a),Buffer.from(b));
const userKey = d => String(d.liker||d.gifter||d.follower||d.member||d.commenter||d.uniqueId||d.userId||d.username||'').replace(/^@/,'').trim().toLowerCase();
const fresh = () => ({latest:{},rankLists:{},events:[],users:{},metrics:{likes:0,diamonds:0,gifts:0,followers:0},dedup:[],version:0});

export function mergeResume(target, incoming) {
  // Reopening an older primary must not discard the recovered backup baseline.
  // Maxima are deliberate: adding two cumulative snapshots would double-count.
  const epochs=target.resetEpochs||={},remote=incoming.resetEpochs||{};
  for(const k of ['likes','gifts','shares'])if(positive(remote[k])>positive(epochs[k])){for(const u of Object.values(target.users))u[k]=0;epochs[k]=remote[k];}
  for(const [id,u] of Object.entries(incoming.users||{})){
    let local=Object.prototype.hasOwnProperty.call(target.users,id)?target.users[id]:null;
    if(!local && Object.keys(target.users).length<500){local={...u,likes:0,gifts:0,shares:0};Object.defineProperty(target.users,id,{value:local,writable:true,enumerable:true,configurable:true});}
    if(local)for(const field of ['likes','gifts','shares'])if(positive(remote[field])>=positive(epochs[field]))local[field]=Math.max(positive(local[field]),positive(u[field]));
  }
  for(const k of Object.keys(target.metrics))target.metrics[k]=Math.max(positive(target.metrics[k]),positive(incoming.metrics?.[k]));
  for(const [kind,entry] of Object.entries(incoming.latest||{}))if(Number(String(entry.revision).split(':')[0])>Number(String(target.latest[kind]?.revision||'0').split(':')[0]))target.latest[kind]=entry;
  target.dedup=[...new Set([...target.dedup,...incoming.dedup||[]])].slice(-2000);
  const events=new Map([...target.events,...incoming.events||[]].map(e=>[e.revision,e]));
  target.events=[...events.values()].filter(e=>Number(String(e.revision).split(':')[0])>Date.now()-90000).sort((a,b)=>Number(a.revision.split(':')[0])-Number(b.revision.split(':')[0])).slice(-100);
  target.version++;
}

export function ingest(session,event,payload) {
  const kinds={tiktok_like:'like',tiktok_gift:'gift',tiktok_follow:'follow',tiktok_share:'share',tiktok_join:'join',tiktok_comment:'comment',tiktok_roomUser:'viewers',tiktok_roomuser:'viewers'};
  const kind=kinds[event]; if(!kind || !payload || typeof payload!=='object' || payload.__preview) return null;
  if(kind==='gift' && payload.isStreakable && !payload.isStreakEnd) return null;
  const state=session.state;
  const eventId=payload.eventId||payload.msgId;
  const dedupKey=eventId ? event+':'+eventId : null;
  if(dedupKey && state.dedup.includes(dedupKey)) return null;
  if(dedupKey) state.dedup=[...state.dedup,dedupKey].slice(-2000);
  const id=userKey(payload).slice(0,120),data={userId:id,username:payload.nickname||id,nickname:String(payload.nickname||id).slice(0,120)};
  for(const k of ['giftId','giftName','giftPictureUrl','giftImageUrl','repeatCount','diamondCount','totalDiamonds','giftType','isStreakable','isStreakEnd','likeCount','shareCount','comment','viewerCount','userCount','eventId'])if(payload[k]!==undefined)data[k]=typeof payload[k]==='string'?payload[k].slice(0,k==='giftName'?120:k==='comment'?500:1024):payload[k];
  // Canonical counters use event deltas, never the upstream cumulative likes.
  data.userId=id;data.avatar=String(payload.profilePictureUrl||payload.avatar||'').slice(0,1024);
  let user=id&&Object.prototype.hasOwnProperty.call(state.users,id) ? state.users[id] : null;
  if(id&&!user && Object.keys(state.users).length<500){user={userId:id,nickname:data.nickname,avatar:data.avatar,likes:0,gifts:0,shares:0};Object.defineProperty(state.users,id,{value:user,writable:true,enumerable:true,configurable:true});}
  if(user){user.nickname=data.nickname;user.avatar=data.avatar;}
  if(kind==='like'){data.likeCount=positive(payload.likeCount);state.metrics.likes+=data.likeCount;if(user)user.likes+=data.likeCount;}
  if(kind==='gift'){data.value=positive(payload.totalDiamonds??positive(payload.diamondCount)*positive(payload.repeatCount||1));data.giftCount=positive(payload.repeatCount||1);state.metrics.diamonds+=data.value;state.metrics.gifts+=data.giftCount;if(user)user.gifts+=data.value;data.sessionCoins=state.metrics.diamonds;}
  if(kind==='share'){data.shareCount=positive(payload.shareCount||1);if(user)user.shares+=data.shareCount;}
  if(kind==='follow'){state.metrics.followers++;const host=state.latest.streamer?.data; if(host&&Number.isFinite(host.totalFollowers)){host.totalFollowers++;data.totalFollowers=host.totalFollowers;}}
  const revision=Date.now()+':'+randomUUID(),entry={revision,data};state.latest[kind]=entry;
  if(kind==='gift'&&data.value>positive(state.latest.highest?.data?.value))state.latest.highest=entry;
  state.events=[...state.events.filter(e=>Number(e.revision.split(':')[0])>Date.now()-90000),{...entry,kind}].slice(-100);
  state.version++;session.updatedAt=Date.now();return {kind,data:{...data,__revision:revision}};
}

export function installOverlaySessions({io,connectTikTok,liveConnections,roomOf,scheduleDisconnect,roomSize,secret=process.env.OVERLAY_MIGRATION_SECRET||'',directory=process.env.OVERLAY_DATA_DIR||'./overlay-data'}) {
  const sessions=new Map(),dir=resolve(directory),retention=24*60*60*1000;
  if(!secret)console.warn('[overlay] OVERLAY_MIGRATION_SECRET missing: signed cross-server recovery is disabled.');
  mkdirSync(dir,{recursive:true,mode:0o700});
  const file=c=>join(dir,c+'.json');
  const save=s=>{const tmp=file(s.channel)+'.tmp';writeFileSync(tmp,JSON.stringify(s),{mode:0o600});renameSync(tmp,file(s.channel));};
  const get=c=>{if(!token(c))return null;if(sessions.has(c))return sessions.get(c);try{const s=JSON.parse(readFileSync(file(c),'utf8'));if(Date.now()-s.updatedAt>retention||sessions.size>=200)return null;sessions.set(c,s);return s;}catch{return null;}};
  const envelope=s=>{const body=JSON.stringify({channel:s.channel,username:s.username,ownerHash:s.ownerHash,streamRoomId:s.streamRoomId,state:{...s.state,rankLists:{}},updatedAt:s.updatedAt});return {body,signature:secret?createHmac('sha256',secret).update(body).digest('hex'):null};};
  const verify=(resume,c)=>{try{if(!secret||typeof resume?.body!=='string'||resume.body.length>1500000||!equal(resume.signature,createHmac('sha256',secret).update(resume.body).digest('hex')))return null;const s=JSON.parse(resume.body);return s.channel===c && token(c) && Date.now()-s.updatedAt<retention && s.updatedAt<=Date.now()+60000 ? s : null;}catch{return null;}};
  const view=s=>{for(const mode of ['likes','gifts','shares','mixed'])s.state.rankLists[mode]=Object.values(s.state.users).map(u=>({...u,score:mode==='mixed'?u.likes+u.gifts+u.shares:u[mode]})).filter(u=>u.score>0).sort((a,b)=>b.score-a.score).slice(0,50);s.state.rankLists._metrics={...s.state.metrics};return {latest:s.state.latest,rankLists:s.state.rankLists,events:s.state.events};};
  const packet=s=>({snapshot:view(s),resume:envelope(s),username:s.username,version:s.state.version});
  const channelRoom=c=>'overlay:'+c;
  const dirty=new Set();
  const flush=setInterval(()=>{for(const s of sessions.values())if(s.state.likeConfig?.autoReduction&&roomSize(channelRoom(s.channel))&&Date.now()-(s.state.lastReduction||0)>=10000){const fraction=1-s.state.likeConfig.reductionPercent/100;for(const u of Object.values(s.state.users))u.likes=Math.floor(u.likes*fraction);s.state.lastReduction=Date.now();(s.state.resetEpochs||={}).likes=Date.now();s.updatedAt=Date.now();s.state.version++;dirty.add(s.channel);}for(const c of dirty){const s=get(c);if(s){try{io.to(channelRoom(c)).emit('overlay_snapshot',packet(s));save(s);}catch(error){console.error('[overlay] save failed:',error.code||'IO_ERROR');}}dirty.delete(c);}},1000);flush.unref();
  const attached=new WeakSet();
  function attach(username,entry){if(!entry||attached.has(entry.adapter))return;attached.add(entry.adapter);
    entry.adapter.on('tiktok_disconnected',payload=>{for(const s of sessions.values())if(s.username===username)io.to(channelRoom(s.channel)).emit('overlay_status',payload.streamEnded?{ended:true}:{disconnected:true});});
    entry.adapter.on('tiktok_reconnected',()=>{for(const s of sessions.values())if(s.username===username)io.to(channelRoom(s.channel)).emit('overlay_status',{connected:true});});
    const events=['tiktok_like','tiktok_gift','tiktok_follow','tiktok_share','tiktok_join','tiktok_comment','tiktok_roomUser','tiktok_roomuser'];
    for(const ev of events)entry.adapter.on(ev,p=>{for(const s of sessions.values())if(s.username===username && (roomSize(channelRoom(s.channel))>0||Date.now()-s.updatedAt<retention)){const e=ingest(s,ev,p);if(e){dirty.add(s.channel);io.to(channelRoom(s.channel)).emit('overlay_event',e);if(e.kind==='gift'&&s.state.latest.highest?.revision===e.data.__revision)io.to(channelRoom(s.channel)).emit('overlay_event',{kind:'highest',data:e.data});}}});
  }
  // The read token is random 256-bit, and grants read/subscription access only.
  function canRead(auth){return token(auth?.overlayChannel)&&!!(get(auth.overlayChannel)||verify(auth.resume,auth.overlayChannel));}
  function release(socket){const c=socket.data.overlayChannel,s=get(c);socket.leave(channelRoom(c));socket.data.overlayChannel=null;if(s){if(socket.currentJoinedUser===s.username)socket.currentJoinedUser=null;if(roomSize(roomOf(s.username))===0)scheduleDisconnect(s.username);}}
  io.on('connection',socket=>{
    let opening=false;
    socket.on('overlay_open',async(payload={},ack=()=>{})=>{
      if(typeof ack!=='function')ack=()=>{};if(!payload||typeof payload!=='object')payload={};
      if(opening)return ack({ok:false,error:'SESSION_BUSY'});opening=true;
      try{
        const c=payload.channel;if(!token(c))throw Error('INVALID_CHANNEL');
        if(socket.data.overlayReadOnly&&c!==socket.handshake.auth.overlayChannel)throw Error('READ_ONLY_CHANNEL');
        let s=get(c);const supplied=verify(payload.resume,c),importing=!s&&supplied;
        if(s&&supplied&&s.username===supplied.username&&s.ownerHash===supplied.ownerHash&&(!s.streamRoomId||!supplied.streamRoomId||s.streamRoomId===supplied.streamRoomId)){mergeResume(s.state,supplied.state);dirty.add(c);}
        if(!s){if(sessions.size>=200)throw Error('SESSION_CAPACITY');
          if(importing)s=importing;
          else {if(!socket.currentJoinedUser||!token(payload.writeKey))throw Error('OWNER_REQUIRED');s={channel:c,username:socket.currentJoinedUser,ownerHash:hash(payload.writeKey),updatedAt:Date.now(),state:fresh()};}
          sessions.set(c,s);save(s);
        }
        // Existing session is authoritative. Never re-add a baseline on every subscription.
        const owner=token(payload.writeKey)&&equal(s.ownerHash,hash(payload.writeKey));
        if(payload.writeKey&&!owner)throw Error('OWNER_MISMATCH');
        if(owner&&payload.rankConfig)s.state.likeConfig={autoReduction:payload.rankConfig.autoReduction===true,reductionPercent:Math.max(1,Math.min(100,positive(payload.rankConfig.reductionPercent)||10))};
        if(owner&&payload.profile){const profile=payload.profile,previous=s.state.latest.streamer?.data||{};s.state.latest.streamer={revision:Date.now()+':'+randomUUID(),data:{...previous,username:s.username,nickname:String(profile.nickname||s.username).slice(0,120),avatar:String(profile.avatar||previous.avatar||'').slice(0,1024),...(typeof profile.totalFollowers==='number'&&Number.isFinite(profile.totalFollowers)?{totalFollowers:positive(profile.totalFollowers)}:{})}};}
        if(socket.data.overlayChannel&&socket.data.overlayChannel!==c)release(socket);
        const avatar=await connectTikTok(s.username,socket.data.clientIp);
        if(liveConnections.get(s.username)?.adapter.isConnected?.()===false){if(roomSize(roomOf(s.username))===0&&!hasViewers(s.username))scheduleDisconnect(s.username);throw Error('UPSTREAM_RECONNECTING');}
        if(!socket.connected){if(roomSize(roomOf(s.username))===0)scheduleDisconnect(s.username);return;}
        const liveRoom=String(liveConnections.get(s.username)?.adapter._state?.().roomId||'');
        if(liveRoom&&s.streamRoomId&&s.streamRoomId!==liveRoom)s.state=fresh();
        if(liveRoom)s.streamRoomId=liveRoom;
        socket.join(channelRoom(c));if(!owner)socket.currentJoinedUser=s.username;socket.data.overlayChannel=c;socket.data.overlayOwner=owner;
        const entry=liveConnections.get(s.username);attach(s.username,entry);
        if(!s.state.latest.streamer)s.state.latest.streamer={revision:Date.now()+':'+randomUUID(),data:{username:s.username,nickname:s.username,avatar:avatar||''}};
        s.updatedAt=Date.now();dirty.add(c);ack({ok:true,...packet(s)});
      }catch(e){ack({ok:false,error:e.message});}finally{opening=false;}
    });
    socket.on('overlay_stop',(_,ack=()=>{})=>{if(typeof ack!=='function')ack=()=>{};if(!socket.data.overlayOwner)return ack({ok:false,error:'OWNER_REQUIRED'});release(socket);ack({ok:true});});
    socket.on('overlay_rank_command',(payload={},ack=()=>{})=>{if(typeof ack!=='function')ack=()=>{};if(!payload||typeof payload!=='object')payload={};const s=get(socket.data.overlayChannel);if(!s||!socket.data.overlayOwner)return ack({ok:false,error:'OWNER_REQUIRED'});if(['likes','gifts','shares'].includes(payload.reset)){for(const u of Object.values(s.state.users))u[payload.reset]=0;(s.state.resetEpochs||={})[payload.reset]=Date.now();}if(payload.config)s.state.likeConfig={autoReduction:payload.config.autoReduction===true,reductionPercent:Math.max(1,Math.min(100,positive(payload.config.reductionPercent)||10))};s.updatedAt=Date.now();s.state.version++;dirty.add(s.channel);ack({ok:true});});
    // Existing disconnect cleanup still governs the upstream grace period.
  });
  const cleanup=setInterval(()=>{
    for(const [c,s] of sessions)if(!roomSize(channelRoom(c))&&Date.now()-s.updatedAt>retention){sessions.delete(c);try{unlinkSync(file(c));}catch{}}
    for(const name of readdirSync(dir))if(/^[a-f0-9]{64}\.json$/.test(name)){try{if(Date.now()-statSync(join(dir,name)).mtimeMs>retention&&!sessions.has(name.slice(0,64)))unlinkSync(join(dir,name));}catch{}}
  },60000);cleanup.unref();
  function hasViewers(username){return [...sessions.values()].some(s=>s.username===username&&roomSize(channelRoom(s.channel))>0);}
  return {canRead,attach,hasViewers};
}
