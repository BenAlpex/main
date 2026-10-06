const {Redis}=require('@upstash/redis'),C=require('crypto');
const r=new Redis({url:process.env.UPSTASH_REDIS_REST_URL||process.env.KV_REST_API_URL,token:process.env.UPSTASH_REDIS_REST_TOKEN||process.env.KV_REST_API_TOKEN});
const RATE=.00011,WAIT=7000,SHOW=3500,START=10000,E=Date.UTC(2026,0,1),DC=22000,DB=12000,DS=6500,MAXB=50000;
const ADM=(process.env.ADMIN_USER||'').toLowerCase(),APASS=process.env.ADMIN_PASS||'';
const colOf=n=>n===0?0:n<=7?1:2, dur=c=>Math.log(c.cp)/RATE, hash=(p,s)=>'h'+C.scryptSync(p,s,32).toString('hex');
async function credit(n,a){const b=await r.hincrby('u:'+n.toLowerCase(),'bal',a);await r.zadd('lb',{score:b,member:n});return b}
async function debit(u,a){const k='u:'+u.k,b=await r.hincrby(k,'bal',-a);if(b<0){await r.hincrby(k,'bal',a);return false}await r.zadd('lb',{score:b,member:u.n});return true}
async function rl(q,key,lim){const ip=String((q.headers&&q.headers['x-forwarded-for'])||'x').split(',')[0].trim(),k='rl:'+key+':'+ip,c=await r.incr(k);if(c===1)await r.expire(k,60);return c<=lim}
/* CRASH */
async function settleCr(c){
 const b=(await r.hgetall('crb:'+c.id))||{};
 for(const [n,x] of Object.entries(b))if(x.auto>=1.01&&x.auto<=c.cp&&await r.hsetnx('crc:'+c.id,n,x.auto))await credit(n,Math.floor(x.amt*x.auto));
 await r.lpush('crh',c.cp);await r.ltrim('crh',0,13)}
async function crash(now){
 let c=await r.get('cr');
 if(!c||now>=c.start+dur(c)+SHOW){
  const id=c?c.id+1:1,u=C.randomInt(1e9)/1e9,cp=Math.min(500,Math.max(1,Math.floor(96/(1-u))/100)),n={id,start:now+WAIT,cp};
  if(await r.set('crn:'+id,n,{nx:true,ex:3600})){if(c)await settleCr(c);await r.set('cr',n);c=n}else c=await r.get('crn:'+id)}
 return c}
/* DOUBLE */
async function dres(id){const k='dr:'+id;await r.set(k,C.randomInt(15),{nx:true,ex:7200});return +(await r.get(k))}
async function settleD(id){
 if(!await r.set('ds:'+id,1,{nx:true,ex:7200}))return;
 const res=await dres(id),b=(await r.hgetall('dbb:'+id))||{};
 for(const [f,a] of Object.entries(b)){const i=f.lastIndexOf(':'),n=f.slice(0,i),k=+f.slice(i+1);if(k===colOf(res))await credit(n,a*(k?2:14))}
 await r.lpush('dh',res);await r.ltrim('dh',0,9)}
/* PUBLIC STATE (cached 0.8s per instance) */
let cache={t:0,v:null,ttl:0};
async function pub(now){
 if(cache.v&&now-cache.t<cache.ttl)return cache.v;
 const c=await crash(now),id=Math.floor((now-E)/DC),t=(now-E)%DC;
 for(const p of (await r.smembers('dpend'))||[]){if(+p<id||t>=DB+DS){await settleD(+p);await r.srem('dpend',p)}}
 const [cb,cc,db,res,ch,crh,dh,ann]=await Promise.all([r.hgetall('crb:'+c.id),r.hgetall('crc:'+c.id),r.hgetall('dbb:'+id),t>=DB?dres(id):null,r.lrange('chat',0,29),r.lrange('crh',0,13),r.lrange('dh',0,9),r.get('ann')]);
 const tt=now-c.start,cr={id:c.id,start:c.start,bets:Object.entries(cb||{}).map(([n,x])=>({n,amt:x.amt,m:(cc&&cc[n])||0}))};
 if(tt>=dur(c))cr.cp=c.cp;
 const bets=Object.entries(db||{}).map(([f,a])=>{const i=f.lastIndexOf(':');return{n:f.slice(0,i),k:+f.slice(i+1),a}});
 const v={t:now,cr,crh,dbl:{id,bets,res},dh,chat:ch,ann:ann||''};cache={t:now,v,ttl:(tt>=0&&tt<dur(c))?400:900};return v}
async function auth(b){
 if(!b.tok)return null;const h=await r.hgetall('u:'+String(b.n||'').toLowerCase());
 return h&&String(h.tok)===String(b.tok)?{n:String(h.n),k:String(h.n).toLowerCase(),bal:+h.bal,admin:String(h.role)==='admin',ban:+h.ban===1}:false}
module.exports=async(q,s)=>{
 const b=q.body||{},now=Date.now(),ok=o=>s.status(200).json(o),er=m=>s.status(200).json({e:m});
 try{
  const a=b.a;
  if(a==='reg'){
   const n=String(b.n||''),p=String(b.p||'');
   if(!/^\w{3,16}$/.test(n)||p.length<4)return er('Kullanıcı adı 3-16 harf/rakam, şifre en az 4 karakter olmalı');
   const k=n.toLowerCase();if(!await rl(q,'reg',5))return er('Çok fazla deneme, 1 dakika bekle');if(k===ADM)return er('Bu kullanıcı adı alınmış');if(!await r.set('un:'+k,1,{nx:true}))return er('Bu kullanıcı adı alınmış');
   const salt=C.randomBytes(8).toString('hex'),tok='t'+C.randomBytes(16).toString('hex');
   await r.hset('u:'+k,{n,bal:START,salt:'s'+salt,ph:hash(p,salt),tok});await r.zadd('lb',{score:START,member:n});
   return ok({n,tok})}
  if(a==='login'){
   const n=String(b.n||'');
   if(!await rl(q,'login',10))return er('Çok fazla deneme, 1 dakika bekle');
   if(ADM&&n.toLowerCase()===ADM){
    const A=Buffer.from(String(b.p||'')),B=Buffer.from(APASS);
    if(!APASS||A.length!==B.length||!C.timingSafeEqual(A,B))return er('Kullanıcı adı veya şifre hatalı');
    const tk='t'+C.randomBytes(16).toString('hex');
    if(!await r.exists('u:'+ADM))await r.hset('u:'+ADM,{n:process.env.ADMIN_USER,bal:START,salt:'s0',ph:'h0'});
    await r.hset('u:'+ADM,{tok:tk,role:'admin'});return ok({n:process.env.ADMIN_USER,tok:tk})}
   const h=await r.hgetall('u:'+n.toLowerCase());
   if(!h||h.ph!==hash(String(b.p||''),String(h.salt).slice(1)))return er('Kullanıcı adı veya şifre hatalı');
   if(+h.ban===1)return er('Hesabın engellendi');const tok=/^t.{8,}/.test(String(h.tok))?String(h.tok):'t'+C.randomBytes(16).toString('hex');await r.hset('u:'+n.toLowerCase(),{tok});return ok({n:String(h.n),tok})}
  const u=await auth(b);if(u===false)return er('auth');if(u&&u.ban)return er('ban');
  if(a==='state'){
   const v=await pub(now),o={...v};if(u)o.me={bal:u.bal,admin:u.admin};
   if(b.lb){const z=await r.zrange('lb',0,9,{rev:true,withScores:true}),t=[];for(let i=0;i<z.length;i+=2)t.push({n:String(z[i]),b:+z[i+1]});o.top=t}
   o.t=now;return ok(o)}
  if(!u)return er('Giriş yapmalısın');
  if(a==='chat'){
   const m=String(b.m||'').trim().slice(0,200);if(!m)return er('');
   if(!await r.set('cd:'+u.k,1,{nx:true,px:1500}))return er('Çok hızlı yazıyorsun');
   await r.lpush('chat',{n:u.n,m,t:now});await r.ltrim('chat',0,49);return ok({})}
  const amt=Math.floor(+b.amt||0);
  if(a==='cbet'){
   if(amt<10||amt>MAXB)return er('Bahis 10 ile '+MAXB+' arasında olmalı');
   const c=await crash(now);if(now>=c.start)return er('Tur başladı, sonrakini bekle');
   const auto=Math.max(0,Math.min(500,+b.auto||0));
   if(!await debit(u,amt))return er('Yetersiz bakiye');
   if(!await r.hsetnx('crb:'+c.id,u.n,{amt,auto})){await credit(u.n,amt);return er('Zaten bahis koydun')}
   await r.expire('crb:'+c.id,3600);cache.t=0;return ok({})}
  if(a==='ccash'){
   const c=await crash(now),t=now-c.start;if(t<0||t>=dur(c))return er('Çok geç');
   const bet=await r.hget('crb:'+c.id,u.n);if(!bet)return er('Bahsin yok');
   const m=Math.floor(Math.exp(RATE*t)*100)/100;
   if(!await r.hsetnx('crc:'+c.id,u.n,m))return er('Zaten çektin');
   await credit(u.n,Math.floor(bet.amt*m));cache.t=0;return ok({m})}
  if(a==='dbet'){
   const id=Math.floor((now-E)/DC),t=(now-E)%DC,k=+b.c;
   if(t>=DB)return er('Bahis süresi doldu');if(![0,1,2].includes(k))return er('Geçersiz renk');
   if(amt<10||amt>MAXB)return er('Bahis 10 ile '+MAXB+' arasında olmalı');
   const f=u.n+':'+k,cur=+(await r.hget('dbb:'+id,f))||0;if(cur+amt>MAXB)return er('Bir renge en fazla '+MAXB);
   if(!await debit(u,amt))return er('Yetersiz bakiye');
   await r.hincrby('dbb:'+id,f,amt);await r.expire('dbb:'+id,7200);await r.sadd('dpend',id);cache.t=0;return ok({})}
  if(a.startsWith('adm_')){
   if(!u.admin)return er('Yetkisiz');
   const bn=((await r.smembers('banned'))||[]).map(String);
   if(a==='adm_stats')return ok({users:await r.zcard('lb'),banned:bn.length});
   if(a==='adm_users'){const q2=String(b.q||'').toLowerCase(),z=await r.zrange('lb',0,499,{rev:true,withScores:true}),l=[];for(let i=0;i<z.length;i+=2){const n=String(z[i]);if(!q2||n.toLowerCase().includes(q2))l.push({n,b:+z[i+1],ban:bn.includes(n.toLowerCase())})}return ok({l:l.slice(0,100)})}
   if(a==='adm_clear'){await r.del('chat');cache.t=0;return ok({})}
   if(a==='adm_ann'){const m=String(b.m||'').trim().slice(0,200);if(m)await r.set('ann',m);else await r.del('ann');cache.t=0;return ok({})}
   const t=String(b.t||'').toLowerCase(),th=t?await r.hgetall('u:'+t):null;
   if(!th)return er('Kullanıcı bulunamadı');
   if(a==='adm_bal'){const v=Math.floor(+b.amt);if(!isFinite(v))return er('Geçerli sayı gir');
    if(b.mode==='set'){if(v<0)return er('Negatif olamaz');await r.hset('u:'+t,{bal:v});await r.zadd('lb',{score:v,member:String(th.n)})}
    else{if(+th.bal+v<0)return er('Bakiye negatif olamaz');await credit(String(th.n),v)}return ok({})}
   if(a==='adm_ban'){if(String(th.role)==='admin')return er('Admin engellenemez');
    if(b.on){await r.hset('u:'+t,{ban:1,tok:'t'+C.randomBytes(8).toString('hex')});await r.sadd('banned',t)}else{await r.hset('u:'+t,{ban:0});await r.srem('banned',t)}return ok({})}
  }
  return er('?')
 }catch(e){console.error(e);return er('Sunucu hatası')}
};
