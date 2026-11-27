const http = require('http'), crypto = require('crypto');
const { WebSocketServer } = require('ws');
const rooms = new Map();
const room = c => rooms.get(c) || rooms.set(c, { mc: null, webs: new Map(), pending: new Map() }).get(c);
const send = (ws, o) => ws && ws.readyState === 1 && ws.send(JSON.stringify(o));

function run(r, line) {
  return new Promise(done => {
    if (!r.mc) return done(null);
    const id = crypto.randomUUID();
    const timer = setTimeout(() => { r.pending.delete(id); done(null); }, 2000);
    r.pending.set(id, b => { clearTimeout(timer); done(b); });
    send(r.mc, { header: { version: 1, requestId: id, messagePurpose: 'commandRequest', messageType: 'commandRequest' },
      body: { version: 1, origin: { type: 'player' }, commandLine: line } });
  });
}

async function tick(r) {
  const state = new Map();
  await Promise.all([...r.webs.keys()].map(async name => {
    const [q, t] = await Promise.all([run(r, `querytarget @a[name="${name}"]`), run(r, `tag "${name}" list`)]);
    try {
      const p = JSON.parse(q.details)[0];
      const m = t.statusMessage.match(/vc_s_([01])_(\d+)_([01])/);
      if (p && m) state.set(name, { pos: p.position, dim: p.dimension, mic: m[1] === '1', range: +m[2], all: m[3] === '1' });
    } catch (e) {}
  }));
  for (const [me, c] of r.webs) {
    const mine = state.get(me), gains = {};
    for (const [other, s] of state) {
      if (other === me || !c.ok || !r.webs.get(other)?.ok || !s.mic) continue;
      if (s.all) gains[other] = 1;
      else if (mine && mine.dim === s.dim) {
        const d = Math.hypot(mine.pos.x - s.pos.x, mine.pos.y - s.pos.y, mine.pos.z - s.pos.z);
        if (d <= s.range) gains[other] = 1 - 0.85 * d / s.range;
      }
    }
    send(c.ws, { type: 'state', gains, mic: !!(c.ok && mine?.mic), world: !!r.mc, found: !!mine });
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(PAGE);
});
const wss = new WebSocketServer({ noServer: true, maxPayload: 65536 });

server.on('upgrade', (req, sock, head) => {
  const mc = req.url.match(/^\/mc\/(\d{6})\/?$/);
  if (!mc && req.url !== '/web') return sock.destroy();
  wss.handleUpgrade(req, sock, head, ws => {
    ws.on('error', () => {});
    mc ? world(ws, mc[1]) : web(ws);
  });
});

function world(ws, code) {
  const r = room(code);
  if (r.mc) return ws.close(1008, 'room in use');
  r.mc = ws;
  run(r, `say เชื่อมโลกกับห้อง ${code} แล้ว`);
  ws.on('message', d => {
    try { const m = JSON.parse(d), f = r.pending.get(m.header.requestId); if (f) { r.pending.delete(m.header.requestId); f(m.body); } } catch (e) {}
  });
  let alive = true;
  const loop = async () => { if (!alive) return; await tick(r); setTimeout(loop, 400); };
  loop();
  ws.on('close', () => { alive = false; r.mc = null; for (const c of r.webs.values()) send(c.ws, { type: 'state', gains: {}, mic: false, world: false }); });
}

function web(ws) {
  let r, me;
  ws.on('message', async d => {
    let m; try { m = JSON.parse(d); } catch (e) { return; }
    if (m.type === 'join' && !me) {
      if (!/^\d{6}$/.test(m.code) || typeof m.name !== 'string' || !/^[^"\\\n]{1,32}$/.test(m.name)) return send(ws, { type: 'error', text: 'รหัสหรือชื่อไม่ถูกต้อง' });
      const found = room(m.code);
      if (!found.mc) return send(ws, { type: 'error', text: 'ห้องนี้ยังไม่ได้เชื่อมโลก' });
      if (found.webs.has(m.name)) return send(ws, { type: 'error', text: 'ชื่อนี้เข้าห้องอยู่แล้ว' });
      r = found; me = m.name;
      const c = { ws, ok: false, pin: String(crypto.randomInt(1000, 10000)), tries: 0 };
      r.webs.set(me, c);
      const res = await run(r, `tellraw "${me}" {"rawtext":[{"text":"เลขยืนยันไมค์: ${c.pin}"}]}`);
      if (!res || res.statusCode < 0) { send(ws, { type: 'error', text: 'ไม่พบชื่อนี้ในโลก' }); return ws.close(); }
      return send(ws, { type: 'pin' });
    }
    const c = r?.webs.get(me);
    if (!c || c.ws !== ws) return;
    if (m.type === 'pin' && !c.ok) {
      if (m.pin !== c.pin) { send(ws, { type: 'error', text: 'เลขยืนยันไม่ถูกต้อง' }); if (++c.tries >= 5) ws.close(); return; }
      c.ok = true;
      send(ws, { type: 'ok', peers: [...r.webs].filter(([n, x]) => n !== me && x.ok).map(([n]) => n) });
    }
    if (m.type === 'signal' && c.ok) { const to = r.webs.get(m.to); if (to?.ok) send(to.ws, { type: 'signal', from: me, data: m.data }); }
  });
  const beat = setInterval(() => ws.ping(), 25000);
  ws.on('close', () => {
    clearInterval(beat);
    if (r?.webs.get(me)?.ws !== ws) return;
    r.webs.delete(me);
    for (const c of r.webs.values()) send(c.ws, { type: 'left', name: me });
  });
}

const PAGE = `<!doctype html><html lang="th"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ไมค์ Minecraft</title>
<style>:root{color-scheme:light dark}body{font:17px/1.6 system-ui,sans-serif;max-width:30rem;margin:auto;padding:1rem}
input,button{box-sizing:border-box;width:100%;padding:.8rem;margin:.3rem 0;font:inherit}code{display:block;padding:.6rem;border:2px solid;word-break:break-all}#state{font-weight:700}</style>
<h1>ไมค์ Minecraft</h1>
<h2>เจ้าของโลก</h2><button id="make">สร้างห้อง</button><code id="cmd">ยังไม่ได้สร้างห้อง</code><button id="copy">คัดลอกคำสั่ง</button>
<h2>เข้าห้องเสียง</h2><input id="code" inputmode="numeric" maxlength="6" placeholder="เลขห้อง"><input id="nick" placeholder="ชื่อในเกม (ตรงทุกตัว)"><button id="join">เข้าใช้ไมค์</button>
<input id="pin" inputmode="numeric" maxlength="4" placeholder="เลขยืนยันจากในเกม"><button id="ok">ยืนยัน</button><p id="state" role="status"></p>
<script>
const $ = i => document.getElementById(i), peers = new Map();
let ws, mic, ctx;
const say = t => $('state').textContent = t;
const tx = o => ws.send(JSON.stringify(o));
$('make').onclick = () => {
  const c = String(100000 + crypto.getRandomValues(new Uint32Array(1))[0] % 900000);
  $('code').value = c;
  $('cmd').textContent = '/connect ' + (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/mc/' + c;
};
$('copy').onclick = () => navigator.clipboard.writeText($('cmd').textContent);
$('join').onclick = async () => {
  try {
    ctx = ctx || new (window.AudioContext || window.webkitAudioContext)(); ctx.resume();
    mic = mic || await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true}});
  } catch (e) { return say('เปิดไมค์ไม่ได้ ให้อนุญาตไมค์แล้วลองใหม่'); }
  mic.getAudioTracks()[0].enabled = false;
  if (ws) { ws.onclose = null; ws.close(); }
  peers.forEach((p, n) => drop(n));
  ws = new WebSocket(location.origin.replace('http', 'ws') + '/web');
  ws.onopen = () => tx({type:'join', code:$('code').value.trim(), name:$('nick').value.trim()});
  ws.onclose = () => { mic.getAudioTracks()[0].enabled = false; peers.forEach((p, n) => drop(n)); say('หลุดจากห้อง กดเข้าใช้ไมค์อีกครั้ง'); };
  let queue = Promise.resolve();
  ws.onmessage = e => { queue = queue.then(() => handle(JSON.parse(e.data))).catch(console.error); };
};
$('ok').onclick = () => ws && tx({type:'pin', pin:$('pin').value.trim()});
function drop(n){ const p = peers.get(n); if (p) { p.pc.close(); peers.delete(n); } }
function peer(n){
  const pc = new RTCPeerConnection({iceServers:[{urls:'stun:stun.l.google.com:19302'}]});
  const p = {pc, ice:[]}; peers.set(n, p);
  mic.getTracks().forEach(t => pc.addTrack(t, mic));
  pc.onicecandidate = e => e.candidate && tx({type:'signal', to:n, data:{ice:e.candidate}});
  pc.ontrack = e => {
    p.el = new Audio(); p.el.muted = true; p.el.srcObject = e.streams[0]; p.el.play().catch(() => {});
    p.gain = ctx.createGain(); p.gain.gain.value = 0;
    ctx.createMediaStreamSource(e.streams[0]).connect(p.gain).connect(ctx.destination);
  };
  return p;
}
async function handle(m){
  if (m.type === 'error') say(m.text);
  if (m.type === 'pin') say('ดูเลขยืนยันในแชตเกม แล้วกรอกที่นี่');
  if (m.type === 'left') drop(m.name);
  if (m.type === 'ok') {
    say('เชื่อมต่อแล้ว');
    for (const n of m.peers) { const p = peer(n); await p.pc.setLocalDescription(await p.pc.createOffer()); tx({type:'signal', to:n, data:{sdp:p.pc.localDescription}}); }
  }
  if (m.type === 'signal') {
    const d = m.data;
    if (d.sdp?.type === 'offer') drop(m.from);
    const p = peers.get(m.from) || peer(m.from);
    if (d.sdp) {
      await p.pc.setRemoteDescription(d.sdp);
      for (const c of p.ice.splice(0)) await p.pc.addIceCandidate(c);
      if (d.sdp.type === 'offer') { await p.pc.setLocalDescription(await p.pc.createAnswer()); tx({type:'signal', to:m.from, data:{sdp:p.pc.localDescription}}); }
    } else if (p.pc.remoteDescription) await p.pc.addIceCandidate(d.ice); else p.ice.push(d.ice);
  }
  if (m.type === 'state') {
    mic.getAudioTracks()[0].enabled = m.mic;
    peers.forEach((p, n) => { if (p.gain) p.gain.gain.value = m.gains[n] || 0; });
    if (!m.world) say('โลกหลุดการเชื่อมต่อ');
    else if (m.found) say('เชื่อมต่อแล้ว | ไมค์' + (m.mic ? 'เปิด' : 'ปิด'));
  }
}
document.onvisibilitychange = () => ctx && ctx.resume();
</script></html>`;
server.listen(process.env.PORT || 3000, () => console.log('ready'));
