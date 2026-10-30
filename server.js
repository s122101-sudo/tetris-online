// テトリス オンライン対戦サーバー
// ・index.html を配信する
// ・同じルームの参加者どうしの「状態(presence)」を中継する（最大6人）
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const html = fs.readFileSync(path.join(__dirname, 'index.html'));
const MAX = 6;
const NAME = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const KEY = /^[A-Za-z_][A-Za-z0-9_-]{0,63}$/;

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200);
    return res.end('ok');
  }
  if (req.url === '/' || req.url === '/index.html' || req.url.startsWith('/?')) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    return res.end(html);
  }
  res.writeHead(404);
  res.end('not found');
});

const wss = new WebSocketServer({ server, maxPayload: 8192 });
const rooms = new Map(); // ルーム名 -> { name, peers: Map(id -> {ws, presence}), timer }

const newId = () => Math.random().toString(36).slice(2, 10).padEnd(8, '0');

function broadcast(room) {
  const body = JSON.stringify([...room.peers].map(([peer, p]) => ({ peer, presence: p.presence })));
  for (const [id, p] of room.peers) {
    if (p.ws.readyState === 1) {
      p.ws.send('{"t":"peers","me":' + JSON.stringify(id) + ',"peers":' + body + '}');
    }
  }
}

// 短い間隔の更新はまとめて送る（約25回/秒まで）
function mark(room) {
  if (room.timer) return;
  room.timer = setTimeout(() => {
    room.timer = 0;
    broadcast(room);
  }, 40);
}

wss.on('connection', (ws) => {
  let room = null;
  let id = '';
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', () => {});

  ws.on('message', (data) => {
    let m;
    try { m = JSON.parse(data); } catch { return; }
    if (!m || typeof m !== 'object') return;
    ws.isAlive = true;

    if (m.t === 'j' && !room) {
      const name = String(m.room || '');
      if (!NAME.test(name)) { ws.close(); return; }
      let r = rooms.get(name);
      if (!r) { r = { name, peers: new Map(), timer: 0 }; rooms.set(name, r); }
      if (r.peers.size >= MAX) { ws.send('{"t":"full"}'); ws.close(); return; }
      do { id = newId(); } while (r.peers.has(id));
      r.peers.set(id, { ws, presence: {} });
      room = r;
      mark(room);
      return;
    }

    if (m.t === 'p' && room && m.patch && typeof m.patch === 'object' && !Array.isArray(m.patch)) {
      const me = room.peers.get(id);
      const next = { ...me.presence };
      for (const k of Object.keys(m.patch)) {
        if (!KEY.test(k) || k === '__proto__') continue;
        if (m.patch[k] === null) delete next[k];
        else next[k] = m.patch[k];
      }
      if (Buffer.byteLength(JSON.stringify(next)) <= 4096) {
        me.presence = next;
        mark(room);
      }
    }
  });

  ws.on('close', () => {
    if (!room) return;
    room.peers.delete(id);
    if (room.peers.size === 0) {
      clearTimeout(room.timer);
      rooms.delete(room.name);
    } else {
      mark(room);
    }
  });
});

// 切れた接続を片付ける
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

const port = process.env.PORT || 3000;
server.listen(port, '0.0.0.0', () => console.log('listening on ' + port));
