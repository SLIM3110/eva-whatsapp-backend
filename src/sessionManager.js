const { default: makeWASocket, DisconnectReason, useMultiFileAuthState } = require('@whiskeysockets/baileys');
const path = require('path');
const fs = require('fs');
const QRCode = require('qrcode');
const pino = require('pino');

const clients = new Map();
const qrCodes = new Map();
const statuses = new Map();

async function updateSupabaseStatus(agentId, status) {
  try {
    const baseUrl = process.env.SUPABASE_URL || 'https://guwmfmwyqrwvufchkzfc.supabase.co';
    const serviceKey = process.env.SUPABASE_SERVICE_KEY;
    const res = await fetch(
      `${baseUrl}/rest/v1/profiles?id=eq.${agentId}`,
      {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          'apikey': serviceKey,
          'Authorization': `Bearer ${serviceKey}`
        },
        body: JSON.stringify({ whatsapp_session_status: status })
      }
    );
    if (!res.ok) console.error(`Failed to update status:`, await res.text());
    else console.log(`Status updated to ${status} for agent ${agentId}`);
  } catch (e) {
    console.error('Supabase status update error:', e.message);
  }
}

async function initClient(agentId) {
  const sessionsDir = path.join(__dirname, '..', 'sessions', `agent-${agentId}`);
  if (!fs.existsSync(sessionsDir)) fs.mkdirSync(sessionsDir, { recursive: true });

  const { state, saveCreds } = await useMultiFileAuthState(sessionsDir);

  const sock = makeWASocket({
    auth: state,
    printQRInTerminal: false,
    logger: pino({ level: 'silent' }),
    browser: ['EVA Real Estate', 'Chrome', '1.0.0']
  });

  clients.set(agentId, sock);
  statuses.set(agentId, 'pending');

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      const base64 = await QRCode.toDataURL(qr);
      qrCodes.set(agentId, base64);
      statuses.set(agentId, 'pending');
      console.log(`QR ready for agent ${agentId} — scan within 60 seconds`);
      await updateSupabaseStatus(agentId, 'pending');
    }

    if (connection === 'open') {
      statuses.set(agentId, 'connected');
      qrCodes.delete(agentId);
      console.log(`Agent ${agentId} connected`);
      await updateSupabaseStatus(agentId, 'connected');
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
      console.log(`Agent ${agentId} disconnected. Status code: ${statusCode}. Reconnecting: ${shouldReconnect}`);
      statuses.set(agentId, 'disconnected');
      await updateSupabaseStatus(agentId, 'disconnected');

      if (shouldReconnect) {
        console.log(`Reconnecting agent ${agentId} in 5 seconds...`);
        setTimeout(() => initClient(agentId), 5000);
      } else {
        console.log(`Agent ${agentId} logged out — session cleared`);
        clients.delete(agentId);
        qrCodes.delete(agentId);
        const sessionPath = path.join(__dirname, '..', 'sessions', `agent-${agentId}`);
        if (fs.existsSync(sessionPath)) fs.rmSync(sessionPath, { recursive: true });
      }
    }
  });

  return sock;
}

async function restoreAllSessions() {
  const sessionsDir = path.join(__dirname, '..', 'sessions');
  if (!fs.existsSync(sessionsDir)) return;
  const dirs = fs.readdirSync(sessionsDir).filter(d => d.startsWith('agent-'));
  console.log(`Restoring ${dirs.length} sessions...`);
  for (const dir of dirs) {
    const agentId = dir.replace('agent-', '');
    console.log(`Restoring session for agent: ${agentId}`);
    await initClient(agentId);
    await new Promise(r => setTimeout(r, 3000));
  }
}

async function createSession(agentId) {
  if (clients.has(agentId) && statuses.get(agentId) === 'connected') {
    return { status: 'already_connected' };
  }
  if (clients.has(agentId)) {
    try { clients.get(agentId).end(); } catch(e) {}
    clients.delete(agentId);
  }
  await initClient(agentId);
  for (let i = 0; i < 20; i++) {
    await new Promise(r => setTimeout(r, 1000));
    if (qrCodes.has(agentId)) break;
    if (statuses.get(agentId) === 'connected') return { status: 'already_connected' };
  }
  return { qrCode: qrCodes.get(agentId) || null, status: 'pending' };
}

function getStatus(agentId) {
  return statuses.get(agentId) || 'disconnected';
}

function getQR(agentId) {
  return qrCodes.get(agentId) || null;
}

async function sendMessage(agentId, number, message) {
  const sock = clients.get(agentId);
  if (!sock || statuses.get(agentId) !== 'connected') {
    throw new Error('Session not connected for this agent');
  }
  const jid = `${number.replace(/\D/g, '')}@s.whatsapp.net`;
  await sock.sendMessage(jid, { text: message });
  return { messageId: `${agentId}-${Date.now()}`, timestamp: new Date().toISOString() };
}

async function disconnectSession(agentId) {
  const sock = clients.get(agentId);
  if (sock) {
    try { await sock.logout(); } catch(e) {}
  }
  clients.delete(agentId);
  qrCodes.delete(agentId);
  statuses.set(agentId, 'disconnected');
  const sessionPath = path.join(__dirname, '..', 'sessions', `agent-${agentId}`);
  if (fs.existsSync(sessionPath)) fs.rmSync(sessionPath, { recursive: true });
  await updateSupabaseStatus(agentId, 'disconnected');
}

module.exports = {
  createSession, getStatus, getQR,
  sendMessage, disconnectSession, restoreAllSessions
};

async function initClient(agentId) {
  const client = new Client({
    authStrategy: new LocalAuth({
      clientId: `agent-${agentId}`,
      dataPath: path.join(__dirname, '..', 'sessions')
    }),
    puppeteer: {
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
        '--no-zygote',
        '--single-process',
        '--disable-gpu'
      ]
    }
  });

  statuses.set(agentId, 'pending');
  clients.set(agentId, client);

  client.on('qr', async (qr) => {
    const base64 = await QRCode.toDataURL(qr, { 
      errorCorrectionLevel: 'M',
      margin: 2,
      width: 300
    });
    qrCodes.set(agentId, base64);
    statuses.set(agentId, 'pending');
    console.log(`QR ready for agent ${agentId} — scan within 60 seconds`);
    await updateSupabaseStatus(agentId, 'pending');
  });

  client.on('ready', async () => {
    statuses.set(agentId, 'connected');
    qrCodes.delete(agentId);
    console.log(`Agent ${agentId} connected`);
    await updateSupabaseStatus(agentId, 'connected');
  });

  client.on('authenticated', () => {
    statuses.set(agentId, 'connected');
  });

  client.on('disconnected', async (reason) => {
    console.log(`Agent ${agentId} disconnected: ${reason}`);
    statuses.set(agentId, 'disconnected');
    await updateSupabaseStatus(agentId, 'disconnected');
    setTimeout(() => {
      console.log(`Attempting reconnect for agent ${agentId}`);
      initClient(agentId);
    }, 30000);
  });

  client.on('auth_failure', async () => {
    statuses.set(agentId, 'disconnected');
    await updateSupabaseStatus(agentId, 'disconnected');
  });

  client.initialize();
  return client;
}

async function restoreAllSessions() {
  const sessionsDir = path.join(__dirname, '..', 'sessions');
  if (!fs.existsSync(sessionsDir)) return;
  const dirs = fs.readdirSync(sessionsDir).filter(d => d.startsWith('agent-'));

  console.log(`Restoring ${dirs.length} sessions with staggered startup...`);

  for (const dir of dirs) {
    const agentId = dir.replace('agent-', '');
    console.log(`Restoring session for agent: ${agentId}`);
    await initClient(agentId);
    // Wait 3 seconds between each agent to avoid RAM spike
    await new Promise(r => setTimeout(r, 3000));
  }

  console.log('All sessions restored');
}

async function createSession(agentId) {
  if (clients.has(agentId) && statuses.get(agentId) === 'connected') {
    return { status: 'already_connected' };
  }
  if (clients.has(agentId)) {
    try { await clients.get(agentId).destroy(); } catch(e) {}
    clients.delete(agentId);
  }
  await initClient(agentId);
  for (let i = 0; i < 15; i++) {
    await new Promise(r => setTimeout(r, 1000));
    if (qrCodes.has(agentId)) break;
    if (statuses.get(agentId) === 'connected') return { status: 'already_connected' };
  }
  return { qrCode: qrCodes.get(agentId) || null, status: 'pending' };
}

function getStatus(agentId) {
  return statuses.get(agentId) || 'disconnected';
}

function getQR(agentId) {
  return qrCodes.get(agentId) || null;
}

async function sendMessage(agentId, number, message) {
  const client = clients.get(agentId);
  if (!client || statuses.get(agentId) !== 'connected') {
    throw new Error('Session not connected for this agent');
  }

  try {
    const chatId = `${number.replace(/\D/g, '')}@c.us`;
    const result = await client.sendMessage(chatId, message);
    return { messageId: result.id._serialized, timestamp: new Date().toISOString() };
  } catch (err) {
    if (err.message.includes('detached Frame') || err.message.includes('Session closed') || err.message.includes('Target closed')) {
      console.log(`Detached frame for agent ${agentId} — marking disconnected and attempting reinit`);
      statuses.set(agentId, 'disconnected');
      await updateSupabaseStatus(agentId, 'disconnected');
      clients.delete(agentId);
      // Attempt to reinitialise after 5 seconds
      setTimeout(() => initClient(agentId), 5000);
      throw new Error('Session detached — reconnecting automatically. Please rescan QR if this persists.');
    }
    throw err;
  }
}

async function disconnectSession(agentId) {
  const client = clients.get(agentId);
  if (client) {
    try { await client.destroy(); } catch(e) {}
  }
  clients.delete(agentId);
  qrCodes.delete(agentId);
  statuses.set(agentId, 'disconnected');
  const sessionPath = path.join(__dirname, '..', 'sessions', `agent-${agentId}`);
  if (fs.existsSync(sessionPath)) {
    fs.rmSync(sessionPath, { recursive: true });
  }
  await updateSupabaseStatus(agentId, 'disconnected');
}

module.exports = {
  createSession, getStatus, getQR,
  sendMessage, disconnectSession, restoreAllSessions
};
