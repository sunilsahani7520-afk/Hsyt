'use strict';
const express = require('express');
const qrcode  = require('qrcode');
const axios   = require('axios');
const fs      = require('fs');
const path    = require('path');
const pino    = require('pino');

const {
  default: makeWASocket,
  DisconnectReason,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  isJidBroadcast,
} = require('@whiskeysockets/baileys');

const PORT         = process.env.PORT        || 3001;
const WEBHOOK_URL  = process.env.WEBHOOK_URL || '';
const SESSIONS_DIR = path.join(__dirname, 'sessions');
if (!fs.existsSync(SESSIONS_DIR)) fs.mkdirSync(SESSIONS_DIR, { recursive: true });

const logger = pino({ level: 'silent' });
const app    = express();
app.use(express.json({ limit: '10mb' }));

// ── Session registry ─────────────────────────────────────────
const sessions   = new Map();
const queues     = new Map();
const processing = new Set();
const retryCount = new Map(); // Track reconnect attempts

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── Message queue ────────────────────────────────────────────
async function enqueue(sid, phone, message) {
  return new Promise((res, rej) => {
    if (!queues.has(sid)) queues.set(sid, []);
    queues.get(sid).push({ phone, message, res, rej });
    if (!processing.has(sid)) processQueue(sid);
  });
}

async function processQueue(sid) {
  processing.add(sid);
  const q = queues.get(sid) || [];
  while (q.length > 0) {
    const { phone, message, res, rej } = q.shift();
    try { res(await sendDirect(sid, phone, message)); }
    catch (e) { rej(e); }
    // Random delay 2-5 seconds between messages
    await sleep(2000 + Math.floor(Math.random() * 3000));
  }
  processing.delete(sid);
}

// ── Create/restore session ───────────────────────────────────
async function createSession(sid, usePairingCode = false, phoneNumber = '') {
  // Return existing if connected
  if (sessions.has(sid)) {
    const s = sessions.get(sid);
    if (s.status === 'connected') return s;
    if (s.status === 'connecting' && !usePairingCode) return s;
  }

  const dir = path.join(SESSIONS_DIR, sid);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const { state, saveCreds } = await useMultiFileAuthState(dir);
  const { version } = await fetchLatestBaileysVersion();

  const sd = {
    socket:      null,
    status:      'connecting',
    qr:          null,
    pairingCode: null,
    phone:       null,
    qrExpiry:    null,
    retries:     (retryCount.get(sid) || 0),
  };
  sessions.set(sid, sd);

  const sock = makeWASocket({
    version,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    logger,
    printQRInTerminal: false,
    syncFullHistory:   false,
    markOnlineOnConnect: true,
    keepAliveIntervalMs: 30000, // Keep connection alive every 30s
    connectTimeoutMs:    60000,
    retryRequestDelayMs: 2000,
    browser: ['WA-SaaS Pro', 'Chrome', '121.0.0'],
    getMessage: async () => ({ conversation: '' }),
  });

  sd.socket = sock;
  sock.ev.on('creds.update', saveCreds);

  // Pairing code request
  if (usePairingCode && phoneNumber && !sock.authState.creds.registered) {
    await sleep(3000);
    try {
      const clean = phoneNumber.replace(/[^0-9]/g, '');
      const code  = await sock.requestPairingCode(clean);
      sd.pairingCode = code;
      console.log(`[${sid}] Pairing Code: ${code}`);
    } catch (e) {
      console.error(`[${sid}] Pairing error:`, e.message);
    }
  }

  sock.ev.on('connection.update', async upd => {
    const { connection, lastDisconnect, qr } = upd;

    if (qr) {
      try {
        sd.qr       = await qrcode.toDataURL(qr);
        sd.status   = 'connecting';
        sd.qrExpiry = Date.now() + 60000;
        console.log(`[${sid}] New QR generated`);
      } catch (e) {}
    }

    if (connection === 'open') {
      sd.status      = 'connected';
      sd.qr          = null;
      sd.pairingCode = null;
      sd.qrExpiry    = null;
      retryCount.set(sid, 0); // Reset retry count on success
      const ph = sock.user?.id?.split(':')[0] || null;
      sd.phone = ph ? '+' + ph : null;
      console.log(`[${sid}] ✓ Connected — ${sd.phone}`);
      pushWebhook({ type: 'session_update', session_id: sid, status: 'connected', phone: sd.phone });
    }

    if (connection === 'close') {
      const errCode    = lastDisconnect?.error?.output?.statusCode;
      const errMessage = lastDisconnect?.error?.message || '';
      
      console.log(`[${sid}] Connection closed. Code: ${errCode}, Msg: ${errMessage}`);

      // Determine if we should reconnect
      const loggedOut = errCode === DisconnectReason.loggedOut;
      const forbidden = errCode === 403;
      const badSession = errCode === 500 || errMessage.includes('Bad session');

      sd.status = 'disconnected';
      pushWebhook({ type: 'session_update', session_id: sid, status: 'disconnected', phone: null });

      if (loggedOut || forbidden) {
        // Logged out - clear auth
        console.log(`[${sid}] Logged out. Clearing session.`);
        sessions.delete(sid);
        retryCount.set(sid, 0);
        try { fs.rmSync(dir, { recursive: true, force: true }); } catch (e) {}
      } else if (badSession) {
        console.log(`[${sid}] Bad session. Clearing and reconnecting.`);
        sessions.delete(sid);
        try { fs.rmSync(dir, { recursive: true, force: true }); } catch (e) {}
        await sleep(5000);
        createSession(sid).catch(console.error);
      } else {
        // Normal disconnect - reconnect with backoff
        const retries = retryCount.get(sid) || 0;
        const delay   = Math.min(5000 * (retries + 1), 60000); // Max 60s backoff
        retryCount.set(sid, retries + 1);
        console.log(`[${sid}] Reconnecting in ${delay/1000}s (attempt ${retries + 1})`);
        sessions.delete(sid);
        setTimeout(() => createSession(sid).catch(console.error), delay);
      }
    }
  });

  // Incoming messages
  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return;
    for (const msg of messages) {
      if (msg.key.fromMe) continue;
      if (isJidBroadcast && isJidBroadcast(msg.key.remoteJid)) continue;
      const from = msg.key.remoteJid?.replace('@s.whatsapp.net', '') || '';
      const txt  = msg.message?.conversation
                || msg.message?.extendedTextMessage?.text
                || msg.message?.imageMessage?.caption || '';
      if (from && txt) {
        pushWebhook({ type: 'incoming_message', session_id: sid, from: '+' + from, message: txt });
      }
    }
  });

  return sd;
}

// ── Send message ─────────────────────────────────────────────
async function sendDirect(sid, phone, message) {
  const s = sessions.get(sid);
  if (!s || s.status !== 'connected') throw new Error('Session not connected');
  
  // Format JID
  let jid = phone.replace(/\D/g, '');
  if (!jid.endsWith('@s.whatsapp.net')) jid += '@s.whatsapp.net';
  
  await s.socket.sendMessage(jid, { text: message });
  return { success: true };
}

// ── Webhook ──────────────────────────────────────────────────
async function pushWebhook(payload) {
  if (!WEBHOOK_URL) return;
  try { await axios.post(WEBHOOK_URL, payload, { timeout: 5000 }); } catch (e) {}
}

// ── Restore sessions on startup ──────────────────────────────
async function restore() {
  if (!fs.existsSync(SESSIONS_DIR)) return;
  const dirs = fs.readdirSync(SESSIONS_DIR);
  for (const d of dirs) {
    const fp = path.join(SESSIONS_DIR, d);
    if (fs.statSync(fp).isDirectory() && fs.existsSync(path.join(fp, 'creds.json'))) {
      console.log(`[startup] Restoring: ${d}`);
      createSession(d).catch(e => console.error(`[startup] ${d}: ${e.message}`));
      await sleep(2000); // Stagger startup
    }
  }
}

// ── Health check loop ─────────────────────────────────────────
// Ping all connected sessions every 2 minutes to keep alive
setInterval(() => {
  for (const [sid, sd] of sessions.entries()) {
    if (sd.status === 'connected' && sd.socket) {
      try {
        // Send presence update to keep connection alive
        sd.socket.sendPresenceUpdate('available').catch(() => {});
      } catch (e) {}
    }
  }
}, 120000);

// ── Routes ───────────────────────────────────────────────────
app.get('/health', (req, res) => {
  const list = [];
  for (const [id, sd] of sessions.entries()) {
    list.push({ session_id: id, status: sd.status, phone: sd.phone });
  }
  res.json({ status: 'online', sessions: sessions.size, uptime: Math.floor(process.uptime()), list });
});

// Start session (QR)
app.post('/session/start', async (req, res) => {
  const { session_id } = req.body;
  if (!session_id) return res.status(400).json({ success: false, error: 'session_id required' });
  try {
    const sd = await createSession(session_id);
    if (sd.status === 'connected') return res.json({ success: true, status: 'connected', phone: sd.phone });
    let w = 0;
    while (!sd.qr && sd.status !== 'connected' && w < 20000) { await sleep(500); w += 500; }
    if (sd.status === 'connected') return res.json({ success: true, status: 'connected', phone: sd.phone });
    if (sd.qr) return res.json({ success: true, qr: sd.qr, status: 'connecting', expires_in: sd.qrExpiry ? Math.max(0, Math.round((sd.qrExpiry - Date.now()) / 1000)) : 60 });
    res.status(503).json({ success: false, error: 'QR timeout — try again' });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// Pairing code
app.post('/session/pair', async (req, res) => {
  const { session_id, phone } = req.body;
  if (!session_id || !phone) return res.status(400).json({ success: false, error: 'session_id and phone required' });
  // Clear existing session
  if (sessions.has(session_id)) {
    const old = sessions.get(session_id);
    if (old.socket) { try { old.socket.end(); } catch (e) {} }
    sessions.delete(session_id);
    const dir = path.join(SESSIONS_DIR, session_id);
    try { fs.rmSync(dir, { recursive: true, force: true }); } catch (e) {}
    await sleep(2000);
  }
  try {
    const sd = await createSession(session_id, true, phone);
    let w = 0;
    while (!sd.pairingCode && sd.status !== 'connected' && w < 20000) { await sleep(500); w += 500; }
    if (sd.status === 'connected') return res.json({ success: true, status: 'connected', phone: sd.phone });
    if (sd.pairingCode) return res.json({ success: true, pairing_code: sd.pairingCode, status: 'connecting' });
    res.status(503).json({ success: false, error: 'Could not get pairing code. Try again.' });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// Status
app.post('/session/status', (req, res) => {
  const { session_id } = req.body;
  const sd = sessions.get(session_id);
  if (!sd) return res.json({ success: true, status: 'disconnected', phone: null, qr: null });
  res.json({
    success:      true,
    status:       sd.status,
    phone:        sd.phone,
    qr:           sd.qr || null,
    pairing_code: sd.pairingCode || null,
    expires_in:   sd.qrExpiry ? Math.max(0, Math.round((sd.qrExpiry - Date.now()) / 1000)) : null,
  });
});

// Disconnect
app.post('/session/disconnect', async (req, res) => {
  const { session_id } = req.body;
  const sd = sessions.get(session_id);
  if (sd?.socket) { try { await sd.socket.logout(); } catch (e) {} }
  sessions.delete(session_id);
  retryCount.set(session_id, 0);
  res.json({ success: true });
});

// Send message
app.post('/send', async (req, res) => {
  const { session_id, phone, message } = req.body;
  if (!session_id || !phone || !message)
    return res.status(400).json({ success: false, error: 'session_id, phone and message required' });
  const sd = sessions.get(session_id);
  if (!sd || sd.status !== 'connected')
    return res.status(503).json({ success: false, error: 'Session not connected — reconnect WhatsApp' });
  try {
    const r = await enqueue(session_id, phone, message);
    res.json(r);
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// List all sessions
app.get('/sessions', (req, res) => {
  const list = [];
  for (const [id, sd] of sessions.entries())
    list.push({ session_id: id, status: sd.status, phone: sd.phone });
  res.json({ success: true, sessions: list });
});

// ── Start server ─────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`\n🚀 WA-SaaS Pro Engine — Port ${PORT}`);
  console.log(`   Webhook: ${WEBHOOK_URL || 'not set'}`);
  console.log(`   Sessions: ${SESSIONS_DIR}\n`);
  await restore();
});

process.on('uncaughtException',  e => console.error('Uncaught:', e.message));
process.on('unhandledRejection', r => console.error('Unhandled:', r));
