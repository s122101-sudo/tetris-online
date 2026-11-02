// テトリス オンライン対戦サーバー（レート戦つき）
// ・index.html を配信する
// ・同じルームの参加者どうしの「状態(presence)」を中継する（最大7人）
// ・対戦の結果からレートを計算し、GitHubのデータ用リポジトリ(ratings.json)に保存する
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const html = fs.readFileSync(path.join(__dirname, 'index.html'));
const MAX = 7; // 1ルームの最大人数
const MAXROOMS = 10; // 同時に存在できるルームの最大数
const NAME = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const KEY = /^[A-Za-z_][A-Za-z0-9_-]{0,63}$/;
const UID = /^[a-f0-9]{32}$/;
const SAVE_DELAY = Number(process.env.SAVE_DELAY_MS) || 10000; // 変更からGitHubへ書くまでの待ち時間
const MIN_ROUND = process.env.MIN_ROUND_MS !== undefined ? Number(process.env.MIN_ROUND_MS) : 15000; // これより短い対戦はレートに入れない

// ===================== レートの保存（GitHub） =====================
const GH_TOKEN = process.env.GITHUB_TOKEN;
const GH_REPO = process.env.GITHUB_REPO; // 例: ユーザー名/tetris-data
const GH_PATH = process.env.GITHUB_PATH || 'ratings.json';
const persistent = !!(GH_TOKEN && GH_REPO);
// players: { プレイヤーID: { r: レート, g: 対戦数, u: 最終更新 } }
const store = { players: {}, ready: !persistent, sha: null, version: 0, saved: 0, busy: false, timer: 0 };

const apiUrl = () => `https://api.github.com/repos/${GH_REPO}/contents/${GH_PATH}`;
const ghHeaders = () => ({
  Authorization: 'Bearer ' + GH_TOKEN,
  Accept: 'application/vnd.github+json',
  'X-GitHub-Api-Version': '2022-11-28',
  'User-Agent': 'tetris-online',
  'Content-Type': 'application/json',
});

async function loadRatings() {
  try {
    const res = await fetch(apiUrl(), { headers: ghHeaders() });
    if (res.status === 404) { store.ready = true; console.log('ratings: 新しいファイルから始めます'); return; }
    if (!res.ok) throw new Error('GitHub ' + res.status);
    const j = await res.json();
    const data = JSON.parse(Buffer.from(j.content, 'base64').toString('utf8'));
    store.players = data && typeof data.players === 'object' && data.players ? data.players : {};
    store.sha = j.sha;
    store.ready = true;
    console.log('ratings: 読み込み完了 (' + Object.keys(store.players).length + '人)');
  } catch (e) {
    // 読み込めないまま上書きしないよう、成功するまで対戦をレートに入れない
    console.error('ratings: 読み込み失敗 -', e.message);
    setTimeout(loadRatings, 30000);
  }
}

function putRatings() {
  const body = {
    message: 'update ratings',
    content: Buffer.from(JSON.stringify({ v: 1, players: store.players })).toString('base64'),
  };
  if (store.sha) body.sha = store.sha;
  return fetch(apiUrl(), { method: 'PUT', headers: ghHeaders(), body: JSON.stringify(body) });
}

async function flushRatings() {
  if (!persistent || !store.ready || store.busy || store.saved === store.version) return;
  store.busy = true;
  const v = store.version;
  try {
    let res = await putRatings();
    if (res.status === 409 || res.status === 422) { // sha が古い → 最新を取り直して書き直す
      const g = await fetch(apiUrl(), { headers: ghHeaders() });
      if (g.ok) { store.sha = (await g.json()).sha; res = await putRatings(); }
    }
    if (!res.ok) throw new Error('GitHub ' + res.status);
    store.sha = (await res.json()).content.sha;
    store.saved = v;
  } catch (e) {
    console.error('ratings: 保存失敗 -', e.message);
  } finally {
    store.busy = false;
  }
}

function scheduleSave(ms) {
  if (!persistent || store.timer) return;
  store.timer = setTimeout(async () => {
    store.timer = 0;
    await flushRatings();
    if (store.saved !== store.version) scheduleSave(30000); // 失敗したらあとで再挑戦
  }, ms);
}

function markDirty() { store.version++; scheduleSave(SAVE_DELAY); }

// サーバーが止まる前に、書き残しがないようにする
async function drain() {
  for (let i = 0; i < 50 && persistent && store.ready && (store.busy || store.saved !== store.version); i++) {
    await flushRatings();
    await new Promise((r) => setTimeout(r, 100));
  }
}

const rtOf = (uid) => (uid && store.ready ? (store.players[uid] ? store.players[uid].r : 1000) : null);
// n人中 k位 の増減。1位が +8n、最下位が -8n、真ん中は±0
const delta = (n, k) => Math.round((8 * n * (n + 1 - 2 * k)) / (n - 1));

// プレイヤーIDと接続の対応（結果の通知用。IDは他のプレイヤーには送らない）
const uidSockets = new Map();
function bindUid(ws, uid) {
  if (!UID.test(uid || '')) return null;
  ws.uid = uid;
  let s = uidSockets.get(uid);
  if (!s) { s = new Set(); uidSockets.set(uid, s); }
  s.add(ws);
  return uid;
}
function unbindUid(ws) {
  if (!ws.uid) return;
  const s = uidSockets.get(ws.uid);
  if (s) { s.delete(ws); if (!s.size) uidSockets.delete(ws.uid); }
}
function sendUid(uid, obj) {
  const s = uidSockets.get(uid);
  if (!s) return;
  const msg = JSON.stringify(obj);
  for (const w of s) if (w.readyState === 1) w.send(msg);
}
const meMsg = (uid) => ({ t: 'me', rt: rtOf(uid), rated: persistent });

// ===================== サーバー本体 =====================
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
const rooms = new Map(); // ルーム名 -> { name, peers: Map(id -> {ws, presence, uid}), timer, round, lastRd }
const lobby = new Set(); // ルーム一覧を見ているだけの接続

const roomList = () => JSON.stringify({
  t: 'rooms', max: MAXROOMS, cap: MAX,
  rooms: [...rooms.values()].map((r) => ({
    name: r.name,
    n: r.peers.size,
    playing: [...r.peers.values()].some((p) => p.presence.run === 1),
  })),
});

let listTimer = 0;
function markList() {
  if (listTimer) return;
  listTimer = setTimeout(() => {
    listTimer = 0;
    const msg = roomList();
    for (const ws of lobby) if (ws.readyState === 1) ws.send(msg);
  }, 300);
}

const newId = () => Math.random().toString(36).slice(2, 10).padEnd(8, '0');

function broadcast(room) {
  const body = JSON.stringify([...room.peers].map(([peer, p]) => ({ peer, presence: p.presence, rt: rtOf(p.uid) })));
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

// ---------- 対戦（ラウンド）の記録とレート計算 ----------
// 対戦開始の合図（go=1, pl=参加者, rd=回数）を見て記録を始め、
// 各自の dead=1 と退出を「脱落」として順番に数え、残り1人以下で順位を確定する。
function trackRound(room, id, pres) {
  if (pres.go === 1 && Array.isArray(pres.pl) && Number.isInteger(pres.rd) && pres.rd > (room.lastRd | 0)) {
    const pl = [...new Set(pres.pl.filter((x) => typeof x === 'string' && room.peers.has(x)))];
    if (pl.includes(id) && pl.length >= 2) {
      room.lastRd = pres.rd;
      room.round = {
        rd: pres.rd,
        players: pl,
        uids: Object.fromEntries(pl.map((x) => [x, room.peers.get(x).uid])),
        dead: [],
        t0: Date.now(),
      };
    }
  }
  const r = room.round;
  if (r && pres.dead && pres.rd === r.rd && r.players.includes(id) && !r.dead.includes(id)) {
    r.dead.push(id);
    checkRound(room);
  }
}

function checkRound(room) {
  const r = room.round;
  if (!r) return;
  const alive = r.players.filter((id) => !r.dead.includes(id));
  if (alive.length > 1) return;
  room.round = null;
  if (!store.ready || Date.now() - r.t0 < MIN_ROUND) return; // 短すぎる対戦は記録しない
  const order = [...alive, ...[...r.dead].reverse()]; // 1位から順
  const n = order.length;
  order.forEach((id, i) => {
    const uid = r.uids[id];
    if (!uid) return;
    const old = rtOf(uid);
    const prev = store.players[uid] || { r: 1000, g: 0 };
    const nw = Math.max(0, old + delta(n, i + 1));
    store.players[uid] = { r: nw, g: (prev.g | 0) + 1, u: Date.now() };
    sendUid(uid, { t: 'result', n, place: i + 1, old, delta: nw - old, rt: nw });
    sendUid(uid, meMsg(uid));
  });
  markDirty();
  mark(room);
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

    if (m.t === 'l' && !room) {
      lobby.add(ws);
      const uid = bindUid(ws, m.uid);
      ws.send(roomList());
      ws.send(JSON.stringify(meMsg(uid)));
      return;
    }

    if (m.t === 'j' && !room) {
      const name = String(m.room || '');
      if (!NAME.test(name)) { ws.close(); return; }
      let r = rooms.get(name);
      if (!r) {
        if (rooms.size >= MAXROOMS) { ws.send('{"t":"nomore"}'); ws.close(); return; }
        r = { name, peers: new Map(), timer: 0, round: null, lastRd: 0 };
        rooms.set(name, r);
      }
      if (r.peers.size >= MAX) { ws.send('{"t":"full"}'); ws.close(); return; }
      const uid = UID.test(m.uid || '') ? m.uid : null;
      if (uid && [...r.peers.values()].some((p) => p.uid === uid)) { // 同じアカウントの二重参加は断る
        ws.send('{"t":"dup"}');
        ws.close();
        return;
      }
      do { id = newId(); } while (r.peers.has(id));
      r.peers.set(id, { ws, presence: {}, uid });
      bindUid(ws, uid);
      room = r;
      lobby.delete(ws);
      ws.send(JSON.stringify(meMsg(uid)));
      mark(room);
      markList();
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
        if ('run' in m.patch) markList();
        trackRound(room, id, next);
      }
    }
  });

  ws.on('close', () => {
    lobby.delete(ws);
    unbindUid(ws);
    if (!room) return;
    const r = room.round;
    if (r && r.players.includes(id) && !r.dead.includes(id)) { // 対戦中の退出は脱落として数える
      r.dead.push(id);
      checkRound(room);
    }
    room.peers.delete(id);
    if (room.peers.size === 0) {
      clearTimeout(room.timer);
      rooms.delete(room.name);
    } else {
      mark(room);
    }
    markList();
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

if (persistent) loadRatings();
else console.warn('GITHUB_TOKEN / GITHUB_REPO が未設定のため、レートはサーバーを再起動すると消えます。');

process.on('SIGTERM', () => { drain().finally(() => process.exit(0)); });

const port = process.env.PORT || 3000;
server.listen(port, '0.0.0.0', () => console.log('listening on ' + port));
