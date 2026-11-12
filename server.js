/* Servidor PVP de Hack the School (Sector 02)
   Salas de 2 jugadores por código de 5 letras. Retransmite mensajes entre los dos
   validando tipo, formato, tamaño y frecuencia. Uso: npm install && npm start */
'use strict';

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
// Orígenes permitidos separados por coma (ej: "https://miusuario.github.io"). Vacío = cualquiera.
// Con Live Server el origen es http://127.0.0.1:5500 o http://localhost:5500.
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map((s) => s.trim()).filter(Boolean);

const MAX_PAYLOAD = 1024;          // bytes por mensaje
const MAX_MSGS_PER_SEC = 80;       // el cliente manda ~20 pos/s + disparos
const MAX_ROOMS = 500;
const MAX_ROOMS_PER_IP = 5;
const ROOM_IDLE_MS = 3 * 60 * 60 * 1000;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';   // igual que el cliente

/** code -> { host, guest, created } */
const rooms = new Map();

const server = http.createServer((req, res) => {
    // Ruta de salud (Render la usa y sirve para "despertar" el servidor)
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Hack the School PVP server OK\n');
});

const wss = new WebSocketServer({ server, maxPayload: MAX_PAYLOAD, perMessageDeflate: false });

const isNum = (v) => typeof v === 'number' && Number.isFinite(v);

/** Valida y limpia un mensaje de juego. Devuelve el mensaje limpio o null si es inválido. */
function sanitize(m) {
    if (!m || typeof m !== 'object' || typeof m.t !== 'string') return null;
    switch (m.t) {
        case 'pos':
            return isNum(m.x) && isNum(m.y) && Math.abs(m.x) < 1000 && Math.abs(m.y) < 1000
                ? { t: 'pos', x: m.x, y: m.y, a: isNum(m.a) && Math.abs(m.a) < 100 ? m.a : undefined } : null;
        case 'hit':
            return isNum(m.d) ? { t: 'hit', d: Math.min(Math.max(m.d, 0), 300) } : null;
        case 'nade':
            return [m.x, m.y, m.vx, m.vy].every(isNum) && Math.abs(m.vx) < 50 && Math.abs(m.vy) < 50
                ? { t: 'nade', x: m.x, y: m.y, vx: m.vx, vy: m.vy } : null;
        case 'dead':
            return { t: 'dead' };
        case 'ready':
            return {
                t: 'ready',
                name: typeof m.name === 'string' ? m.name.slice(0, 12) : 'JUGADOR',
                color: Number.isInteger(m.color) && m.color >= 0 && m.color < 32 ? m.color : 0,
                style: Number.isInteger(m.style) && m.style >= 0 && m.style < 32 ? m.style : 0,
            };
        case 'hello':
            return Number.isInteger(m.sector) && m.sector >= 0 && m.sector < 8
                ? { t: 'hello', sector: m.sector } : null;
        default:
            return null;
    }
}

const send = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };

function newCode() {
    for (let tries = 0; tries < 50; tries++) {
        let code = '';
        for (let i = 0; i < 5; i++) code += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
        if (!rooms.has(code)) return code;
    }
    return null;
}

function leaveRoom(ws) {
    const code = ws.room;
    if (!code) return;
    ws.room = null;
    const room = rooms.get(code);
    if (!room) return;
    const other = room.host === ws ? room.guest : room.host;
    rooms.delete(code);                         // la sala muere cuando alguien se va
    if (other) { other.room = null; send(other, { type: 'peer-left' }); }
}

const roomsOfIp = (ip) => [...rooms.values()].filter((r) => r.host.ip === ip).length;

wss.on('connection', (ws, req) => {
    const origin = req.headers.origin || '';
    if (ALLOWED_ORIGINS.length && !ALLOWED_ORIGINS.includes(origin)) {
        ws.close(1008, 'origin');
        return;
    }
    // Detrás del proxy de Render la IP real viene en x-forwarded-for
    ws.ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    ws.room = null;
    ws.alive = true;
    ws.msgCount = 0;
    ws.on('pong', () => { ws.alive = true; });

    ws.on('message', (raw, isBinary) => {
        if (isBinary) return;
        if (++ws.msgCount > MAX_MSGS_PER_SEC) { ws.close(1008, 'rate'); return; }
        let m;
        try { m = JSON.parse(raw.toString()); } catch { return; }
        if (!m || typeof m !== 'object') return;

        if (m.type === 'create') {
            if (ws.room) return;
            if (rooms.size >= MAX_ROOMS || roomsOfIp(ws.ip) >= MAX_ROOMS_PER_IP) {
                send(ws, { type: 'error', reason: 'busy' });
                return;
            }
            const code = newCode();
            if (!code) { send(ws, { type: 'error', reason: 'busy' }); return; }
            rooms.set(code, { host: ws, guest: null, created: Date.now() });
            ws.room = code;
            send(ws, { type: 'created', code });
        } else if (m.type === 'join') {
            if (ws.room) return;
            const code = typeof m.code === 'string' ? m.code.toUpperCase().slice(0, 5) : '';
            const room = rooms.get(code);
            if (!room) { send(ws, { type: 'error', reason: 'no-room' }); return; }
            if (room.guest) { send(ws, { type: 'error', reason: 'full' }); return; }
            room.guest = ws;
            ws.room = code;
            send(ws, { type: 'joined' });
            send(room.host, { type: 'peer-joined' });
        } else if (m.type === 'msg') {
            const room = ws.room && rooms.get(ws.room);
            if (!room || !room.host || !room.guest) return;
            const clean = sanitize(m.d);
            if (!clean) return;
            // 'hello' solo lo puede mandar el anfitrión (elige el sector)
            if (clean.t === 'hello' && room.host !== ws) return;
            send(room.host === ws ? room.guest : room.host, { type: 'msg', d: clean });
        }
    });

    ws.on('close', () => leaveRoom(ws));
    ws.on('error', () => leaveRoom(ws));
});

// Tareas periódicas: contador de mensajes, latido (mantiene vivas las conexiones) y limpieza
setInterval(() => { for (const ws of wss.clients) ws.msgCount = 0; }, 1000);
setInterval(() => {
    for (const ws of wss.clients) {
        if (!ws.alive) { ws.terminate(); continue; }
        ws.alive = false;
        ws.ping();
    }
    const now = Date.now();
    for (const [code, room] of rooms) {
        if (now - room.created > ROOM_IDLE_MS) {
            for (const p of [room.host, room.guest]) if (p) p.close(1000, 'timeout');
            rooms.delete(code);
        }
    }
}, 25000);

server.listen(PORT, () => console.log(`PVP server escuchando en el puerto ${PORT}`));
