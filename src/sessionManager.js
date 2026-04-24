const clients = new Map();
const statuses = new Map();

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://guwmfmwyqrwvufchkzfc.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

async function supabaseRequest(path, options = {}) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      ...(options.headers || {})
    }
  });
  return res;
}

async function updateSupabaseStatus(agentId, status) {
  try {
    const res = await supabaseRequest(`/profiles?id=eq.${agentId}`, {
      method: 'PATCH',
      body: JSON.stringify({ whatsapp_session_status: status })
    });
    if (!res.ok) console.error('Failed to update status:', await res.text());
    else console.log(`Status updated to ${status} for agent ${agentId}`);
  } catch (e) {
    console.error('Supabase status update error:', e.message);
  }
}

async function getAgentCredentials(agentId) {
  const res = await supabaseRequest(`/profiles?id=eq.${agentId}&select=green_api_instance_id,green_api_token,green_api_url`);
  const profiles = await res.json();
  if (!profiles.length || !profiles[0].green_api_instance_id) return null;
  return profiles[0];
}

async function registerWebhook(apiUrl, instanceId, token) {
  try {
    const res = await fetch(`${apiUrl}/waInstance${instanceId}/setSettings/${token}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        webhookUrl: 'https://api.evaintelligencehub.online/webhook/incoming',
        webhookUrlToken: '',
        incomingWebhook: 'yes'
      })
    });
    if (!res.ok) {
      const body = await res.text();
      console.error(`Webhook registration failed for instance ${instanceId}: ${body}`);
    } else {
      console.log(`Webhook registered for instance ${instanceId}`);
    }
  } catch (e) {
    console.error(`Webhook registration error for instance ${instanceId}:`, e.message);
  }
}

async function createSession(agentId) {
  const creds = await getAgentCredentials(agentId);
  if (!creds) {
    throw new Error('No Green API instance assigned to this agent. Please contact your administrator.');
  }
  const { green_api_instance_id, green_api_token, green_api_url } = creds;
  clients.set(agentId, { idInstance: green_api_instance_id, apiTokenInstance: green_api_token, apiUrl: green_api_url });

  const stateRes = await fetch(`${green_api_url}/waInstance${green_api_instance_id}/getStateInstance/${green_api_token}`);
  const stateData = await stateRes.json();

  if (stateData.stateInstance === 'authorized') {
    statuses.set(agentId, 'connected');
    await updateSupabaseStatus(agentId, 'connected');
    await registerWebhook(green_api_url, green_api_instance_id, green_api_token);
    return { status: 'already_connected' };
  }

  const qrRes = await fetch(`${green_api_url}/waInstance${green_api_instance_id}/qr/${green_api_token}`);
  const qrData = await qrRes.json();

  if (qrData.type === 'alreadyLogged') {
    statuses.set(agentId, 'connected');
    await updateSupabaseStatus(agentId, 'connected');
    await registerWebhook(green_api_url, green_api_instance_id, green_api_token);
    return { status: 'already_connected' };
  }

  if (qrData.type === 'qrCode') {
    statuses.set(agentId, 'pending');
    return { qrCode: `data:image/png;base64,${qrData.message}`, status: 'pending' };
  }

  return { qrCode: null, status: 'pending' };
}

async function getStatus(agentId) {
  let creds = clients.get(agentId);
  if (!creds) {
    const profile = await getAgentCredentials(agentId);
    if (!profile) return { status: 'disconnected', qrCode: null };
    creds = { idInstance: profile.green_api_instance_id, apiTokenInstance: profile.green_api_token, apiUrl: profile.green_api_url };
    clients.set(agentId, creds);
  }

  try {
    const res = await fetch(`${creds.apiUrl}/waInstance${creds.idInstance}/getStateInstance/${creds.apiTokenInstance}`);
    const data = await res.json();

    if (data.stateInstance === 'authorized') {
      const wasConnected = statuses.get(agentId) === 'connected';
      statuses.set(agentId, 'connected');
      await updateSupabaseStatus(agentId, 'connected');
      if (!wasConnected) {
        await registerWebhook(creds.apiUrl, creds.idInstance, creds.apiTokenInstance);
      }
      return { status: 'connected', qrCode: null };
    }

    if (data.stateInstance === 'notAuthorized') {
      const qrRes = await fetch(`${creds.apiUrl}/waInstance${creds.idInstance}/qr/${creds.apiTokenInstance}`);
      const qrData = await qrRes.json();
      if (qrData.type === 'qrCode') {
        statuses.set(agentId, 'pending');
        return { status: 'pending', qrCode: `data:image/png;base64,${qrData.message}` };
      }
      statuses.set(agentId, 'disconnected');
      return { status: 'disconnected', qrCode: null };
    }

    return { status: statuses.get(agentId) || 'pending', qrCode: null };
  } catch (e) {
    console.error(`Error getting status for agent ${agentId}:`, e.message);
    return { status: statuses.get(agentId) || 'disconnected', qrCode: null };
  }
}

function getQR(agentId) {
  return null;
}

// ── Typing simulation helpers ─────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Calculate a realistic typing delay based on message length.
 * A human typing at ~45 WPM on a phone would take:
 *   short msg  (<100 chars)  → 3–6 s
 *   medium msg (100–300 chars) → 6–12 s
 *   long msg   (300+ chars)  → 10–18 s
 * We add ±20% random jitter on top.
 */
function typingDurationMs(message) {
  const len = (message || '').length;
  let base;
  if (len < 100)       base = 3000 + Math.random() * 3000;   // 3–6 s
  else if (len < 300)  base = 6000 + Math.random() * 6000;   // 6–12 s
  else                 base = 10000 + Math.random() * 8000;  // 10–18 s
  // ±20% jitter
  const jitter = base * 0.2 * (Math.random() - 0.5);
  return Math.round(base + jitter);
}

/**
 * Send the "typing…" chat action to Green API.
 * This shows the typing indicator in the recipient's chat.
 * Errors are silently ignored — it's cosmetic, not critical.
 */
async function sendTypingAction(creds, chatId) {
  try {
    await fetch(
      `${creds.apiUrl}/waInstance${creds.idInstance}/sendChatAction/${creds.apiTokenInstance}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chatId, message: 'typing' })
      }
    );
  } catch (e) {
    // Non-critical — don't let a typing indicator failure block the send
    console.warn(`[typing] sendChatAction failed for ${chatId}:`, e.message);
  }
}

async function sendMessage(agentId, number, message) {
  let creds = clients.get(agentId);
  if (!creds) {
    const profile = await getAgentCredentials(agentId);
    if (!profile) throw new Error('No Green API instance configured for this agent');
    creds = { idInstance: profile.green_api_instance_id, apiTokenInstance: profile.green_api_token, apiUrl: profile.green_api_url };
    clients.set(agentId, creds);
  }

  const cleanNumber = number.replace(/\D/g, '');
  const chatId = `${cleanNumber}@c.us`;

  // 1. Show typing indicator to the recipient
  await sendTypingAction(creds, chatId);

  // 2. Hold for a realistic typing duration before actually sending
  const typingMs = typingDurationMs(message);
  console.log(`[typing] Simulating ${Math.round(typingMs / 1000)}s typing for ${number}`);
  await sleep(typingMs);

  // 3. Send the message
  const res = await fetch(`${creds.apiUrl}/waInstance${creds.idInstance}/sendMessage/${creds.apiTokenInstance}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chatId, message })
  });

  const data = await res.json();
  if (!res.ok) throw new Error(`Green API send error: ${JSON.stringify(data)}`);

  console.log(`Sent to ${number} for agent ${agentId} via Green API`);
  return { messageId: data.idMessage, timestamp: new Date().toISOString() };
}

/**
 * Send a WhatsApp poll from the agent's Green API instance.
 *
 * Green API endpoint: POST /waInstance{id}/sendPoll/{token}
 * Body: { chatId, message, options: [{optionName}], multipleAnswers }
 *
 * The `quotedMessageId` param is optional — pass it to attach the poll
 * as a reply to the original outreach message, keeping the conversation
 * threaded and making context obvious to the recipient.
 *
 * `typingTimeMs` is passed straight to Green API — it shows the typing
 * indicator on their end for that many milliseconds before the poll appears.
 */
async function sendPoll(agentId, number, question, options, quotedMessageId = null) {
  let creds = clients.get(agentId);
  if (!creds) {
    const profile = await getAgentCredentials(agentId);
    if (!profile) throw new Error('No Green API instance configured for this agent');
    creds = { idInstance: profile.green_api_instance_id, apiTokenInstance: profile.green_api_token, apiUrl: profile.green_api_url };
    clients.set(agentId, creds);
  }

  const cleanNumber = number.replace(/\D/g, '');
  const chatId      = `${cleanNumber}@c.us`;

  // Realistic typing pause before the poll appears (2–5 s)
  const typingMs = 2000 + Math.floor(Math.random() * 3000);

  const body = {
    chatId,
    message:         question,
    options:         options.map(o => ({ optionName: o })),
    multipleAnswers: false,
    typingTime:      typingMs,
  };

  // Thread the poll as a reply to the original message if we have its ID
  if (quotedMessageId) {
    body.quotedMessageId = quotedMessageId;
  }

  const res = await fetch(
    `${creds.apiUrl}/waInstance${creds.idInstance}/sendPoll/${creds.apiTokenInstance}`,
    {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(body),
    }
  );

  const data = await res.json();
  if (!res.ok) throw new Error(`Green API sendPoll error: ${JSON.stringify(data)}`);

  console.log(`Poll sent to ${number} for agent ${agentId}`);
  return { messageId: data.idMessage, timestamp: new Date().toISOString() };
}

async function disconnectSession(agentId) {
  const creds = clients.get(agentId);
  if (creds) {
    try {
      await fetch(`${creds.apiUrl}/waInstance${creds.idInstance}/logout/${creds.apiTokenInstance}`);
      console.log(`Agent ${agentId} logged out from Green API`);
    } catch(e) {}
  }
  clients.delete(agentId);
  statuses.set(agentId, 'disconnected');
  await updateSupabaseStatus(agentId, 'disconnected');
}

async function restoreAllSessions() {
  console.log('Green API sessions are managed on Green API servers — no local restore needed');
}

module.exports = { createSession, getStatus, getQR, sendMessage, sendPoll, disconnectSession, restoreAllSessions };
