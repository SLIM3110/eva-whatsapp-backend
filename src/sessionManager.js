const clients = new Map();
const statuses = new Map();

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://guwmfmwyqrwvufchkzfc.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

// Safe JSON parse -- returns null if body is empty or invalid
async function safeJson(res) {
  try {
    const text = await res.text();
    if (!text || !text.trim()) return null;
    return JSON.parse(text);
  } catch (e) {
    return null;
  }
}

async function supabaseRequest(path, options) {
  options = options || {};
  const res = await fetch(SUPABASE_URL + '/rest/v1' + path, Object.assign({}, options, {
    headers: Object.assign({
      'Content-Type': 'application/json',
      'apikey': SUPABASE_KEY,
      'Authorization': 'Bearer ' + SUPABASE_KEY,
    }, options.headers || {}),
  }));
  return res;
}

async function updateSupabaseStatus(agentId, status) {
  try {
    const res = await supabaseRequest('/profiles?id=eq.' + agentId, {
      method: 'PATCH',
      body: JSON.stringify({ whatsapp_session_status: status })
    });
    if (!res.ok) console.error('Failed to update status:', await res.text());
    else console.log('Status updated to ' + status + ' for agent ' + agentId);
  } catch (e) {
    console.error('Supabase status update error:', e.message);
  }
}

async function getAgentCredentials(agentId) {
  const res = await supabaseRequest('/profiles?id=eq.' + agentId + '&select=green_api_instance_id,green_api_token,green_api_url');
  const profiles = await res.json();
  if (!profiles.length || !profiles[0].green_api_instance_id) return null;
  return profiles[0];
}

async function registerWebhook(apiUrl, instanceId, token) {
  try {
    const res = await fetch(apiUrl + '/waInstance' + instanceId + '/setSettings/' + token, {
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
      console.error('Webhook registration failed for instance ' + instanceId + ': ' + body);
    } else {
      console.log('Webhook registered for instance ' + instanceId);
    }
  } catch (e) {
    console.error('Webhook registration error for instance ' + instanceId + ':', e.message);
  }
}

async function createSession(agentId) {
  const creds = await getAgentCredentials(agentId);
  if (!creds) {
    throw new Error('No Green API instance assigned to this agent. Please contact your administrator.');
  }
  const green_api_instance_id = creds.green_api_instance_id;
  const green_api_token       = creds.green_api_token;
  const green_api_url         = creds.green_api_url;
  clients.set(agentId, { idInstance: green_api_instance_id, apiTokenInstance: green_api_token, apiUrl: green_api_url });

  const stateRes  = await fetch(green_api_url + '/waInstance' + green_api_instance_id + '/getStateInstance/' + green_api_token);
  const stateData = await safeJson(stateRes);
  if (!stateData) { statuses.set(agentId, 'pending'); return { qrCode: null, status: 'pending' }; }

  if (stateData.stateInstance === 'authorized') {
    statuses.set(agentId, 'connected');
    await updateSupabaseStatus(agentId, 'connected');
    await registerWebhook(green_api_url, green_api_instance_id, green_api_token);
    return { status: 'already_connected' };
  }

  const qrRes  = await fetch(green_api_url + '/waInstance' + green_api_instance_id + '/qr/' + green_api_token);
  const qrData = await safeJson(qrRes);

  if (qrData && qrData.type === 'alreadyLogged') {
    statuses.set(agentId, 'connected');
    await updateSupabaseStatus(agentId, 'connected');
    await registerWebhook(green_api_url, green_api_instance_id, green_api_token);
    return { status: 'already_connected' };
  }

  if (qrData && qrData.type === 'qrCode') {
    statuses.set(agentId, 'pending');
    return { qrCode: 'data:image/png;base64,' + qrData.message, status: 'pending' };
  }

  return { qrCode: null, status: 'pending' };
}

async function getStatus(agentId) {
  var creds = clients.get(agentId);
  if (!creds) {
    const profile = await getAgentCredentials(agentId);
    if (!profile) return { status: 'disconnected', qrCode: null };
    creds = { idInstance: profile.green_api_instance_id, apiTokenInstance: profile.green_api_token, apiUrl: profile.green_api_url };
    clients.set(agentId, creds);
  }

  try {
    const res  = await fetch(creds.apiUrl + '/waInstance' + creds.idInstance + '/getStateInstance/' + creds.apiTokenInstance);
    const data = await safeJson(res);
    if (!data) return { status: statuses.get(agentId) || 'disconnected', qrCode: null };

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
      const qrRes  = await fetch(creds.apiUrl + '/waInstance' + creds.idInstance + '/qr/' + creds.apiTokenInstance);
      const qrData = await safeJson(qrRes);
      if (qrData && qrData.type === 'qrCode') {
        statuses.set(agentId, 'pending');
        return { status: 'pending', qrCode: 'data:image/png;base64,' + qrData.message };
      }
      statuses.set(agentId, 'disconnected');
      return { status: 'disconnected', qrCode: null };
    }

    return { status: statuses.get(agentId) || 'pending', qrCode: null };
  } catch (e) {
    console.error('Error getting status for agent ' + agentId + ':', e.message);
    return { status: statuses.get(agentId) || 'disconnected', qrCode: null };
  }
}

function getQR(agentId) {
  return null;
}

// Typing simulation helpers

function sleep(ms) {
  return new Promise(function(resolve) { setTimeout(resolve, ms); });
}

function typingDurationMs(message) {
  const len = (message || '').length;
  var base;
  if (len < 100)       base = 3000 + Math.random() * 3000;
  else if (len < 300)  base = 6000 + Math.random() * 6000;
  else                 base = 10000 + Math.random() * 8000;
  const jitter = base * 0.2 * (Math.random() - 0.5);
  return Math.round(base + jitter);
}

async function sendTypingAction(creds, chatId) {
  try {
    await fetch(
      creds.apiUrl + '/waInstance' + creds.idInstance + '/sendChatAction/' + creds.apiTokenInstance,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chatId: chatId, message: 'typing' })
      }
    );
  } catch (e) {
    console.warn('[typing] sendChatAction failed for ' + chatId + ':', e.message);
  }
}

async function sendMessage(agentId, number, message) {
  var creds = clients.get(agentId);
  if (!creds) {
    const profile = await getAgentCredentials(agentId);
    if (!profile) throw new Error('No Green API instance configured for this agent');
    creds = { idInstance: profile.green_api_instance_id, apiTokenInstance: profile.green_api_token, apiUrl: profile.green_api_url };
    clients.set(agentId, creds);
  }

  const cleanNumber = number.replace(/\D/g, '');
  const chatId      = cleanNumber + '@c.us';

  await sendTypingAction(creds, chatId);

  const typingMs = typingDurationMs(message);
  console.log('[typing] Simulating ' + Math.round(typingMs / 1000) + 's typing for ' + number);
  await sleep(typingMs);

  const res = await fetch(
    creds.apiUrl + '/waInstance' + creds.idInstance + '/sendMessage/' + creds.apiTokenInstance,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chatId: chatId, message: message })
    }
  );

  const data = await res.json();
  if (!res.ok) throw new Error('Green API send error: ' + JSON.stringify(data));

  console.log('Sent to ' + number + ' for agent ' + agentId + ' via Green API');
  return { messageId: data.idMessage, timestamp: new Date().toISOString() };
}

/**
 * Send a WhatsApp message with tap-to-reply buttons.
 *
 * Green API endpoint: POST /waInstance{id}/sendButtons/{token}
 * Body: { chatId, message, footer, buttons: [{buttonId, buttonText}] }
 *
 * Supports up to 3 buttons. We use 3 preset options:
 *   btn_rent   -> Rent it out
 *   btn_sell   -> Sell it
 *   btn_remove -> Remove me
 *
 * The outreach message text goes in the `message` field above the buttons.
 */
async function sendButtons(agentId, number, message) {
  var creds = clients.get(agentId);
  if (!creds) {
    const profile = await getAgentCredentials(agentId);
    if (!profile) throw new Error('No Green API instance configured for this agent');
    creds = { idInstance: profile.green_api_instance_id, apiTokenInstance: profile.green_api_token, apiUrl: profile.green_api_url };
    clients.set(agentId, creds);
  }

  const cleanNumber = number.replace(/\D/g, '');
  const chatId      = cleanNumber + '@c.us';

  // Show typing indicator before the message appears
  await sendTypingAction(creds, chatId);
  const typingMs = 2000 + Math.floor(Math.random() * 3000);
  await sleep(typingMs);

  const body = {
    chatId:   chatId,
    message:  message,
    footer:   'Tap a reply below, or just send a message.',
    buttons: [
      { buttonId: 'btn_rent',           buttonText: 'Rent' },
      { buttonId: 'btn_sell',           buttonText: 'Sell' },
      { buttonId: 'btn_not_interested', buttonText: 'Not interested' },
    ],
  };

  const res = await fetch(
    creds.apiUrl + '/waInstance' + creds.idInstance + '/sendButtons/' + creds.apiTokenInstance,
    {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(body),
    }
  );

  const data = await res.json();
  if (!res.ok) throw new Error('Green API sendButtons error: ' + JSON.stringify(data));

  console.log('Button message sent to ' + number + ' for agent ' + agentId);
  return { messageId: data.idMessage, timestamp: new Date().toISOString() };
}

async function disconnectSession(agentId) {
  const creds = clients.get(agentId);
  if (creds) {
    try {
      await fetch(creds.apiUrl + '/waInstance' + creds.idInstance + '/logout/' + creds.apiTokenInstance);
      console.log('Agent ' + agentId + ' logged out from Green API');
    } catch(e) {}
  }
  clients.delete(agentId);
  statuses.set(agentId, 'disconnected');
  await updateSupabaseStatus(agentId, 'disconnected');
}

async function restoreAllSessions() {
  console.log('Green API sessions are managed on Green API servers -- no local restore needed');
}

module.exports = { createSession, getStatus, getQR, sendMessage, sendButtons, disconnectSession, restoreAllSessions };
