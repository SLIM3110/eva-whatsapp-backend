const sessionManager = require('./sessionManager');

// SET THIS TO true FOR TESTING, false FOR PRODUCTION
const TEST_MODE = false;

const FETCH_TIMEOUT_MS = 10000;

// Strip all non-digits from a phone number string
function normalizePhone(num) {
  return String(num || '').replace(/\D/g, '');
}

// Return every plausible formatting variant of a number so we catch
// mismatches like +971501234567 / 971501234567 / 0501234567 / 00971501234567
function phoneVariants(num) {
  const digits = normalizePhone(num);
  const variants = new Set([digits, '+' + digits]);
  // UAE country code stripping
  if (digits.startsWith('971') && digits.length > 9) {
    const local = digits.slice(3);
    variants.add(local);
    variants.add('0' + local);
    variants.add('00971' + local);
  } else if (digits.startsWith('00971') && digits.length > 11) {
    const local = digits.slice(5);
    variants.add(local);
    variants.add('0' + local);
    variants.add('971' + local);
    variants.add('+971' + local);
  } else if (digits.startsWith('0') && digits.length >= 9) {
    const local = digits.slice(1);
    variants.add(local);
    variants.add('971' + local);
    variants.add('+971' + local);
    variants.add('00971' + local);
  }
  return Array.from(variants);
}

async function supabaseFetch(path, options = {}) {
  const baseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;

  if (!baseUrl) throw new Error('SUPABASE_URL is not set in Railway variables');
  if (!serviceKey) throw new Error('SUPABASE_SERVICE_KEY is not set in Railway variables');

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const res = await fetch(`${baseUrl}/rest/v1${path}`, {
      ...options,
      signal: controller.signal,
      headers: {
        'apikey': serviceKey,
        'Authorization': `Bearer ${serviceKey}`,
        'Content-Type': 'application/json',
        ...(options.headers || {})
      }
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Supabase ${options.method || 'GET'} ${path} failed [${res.status}]: ${body}`);
    }
    return res;
  } finally {
    clearTimeout(timer);
  }
}

async function tick() {
  if (!TEST_MODE) {
    const uaeTime = new Date().toLocaleString('en-US', { timeZone: 'Asia/Dubai' });
    const uaeHour = new Date(uaeTime).getHours();
    if (uaeHour < 9 || uaeHour >= 21) {
      console.log(`Scheduler: outside UAE business hours (${uaeHour}:00), skipping`);
      return;
    }
  }

  const agentsRes = await supabaseFetch(
    '/profiles?whatsapp_session_status=eq.connected&is_active=eq.true&role=in.(agent,super_admin)&select=id'
  );
  const agents = await agentsRes.json();
  if (!Array.isArray(agents)) return;

  // Process all agents in parallel
  await Promise.all(
    agents.map(agent =>
      processAgent(agent.id).catch(e =>
        console.error(`Scheduler error for agent ${agent.id}:`, e.message)
      )
    )
  );
}

async function processAgent(agentId) {
  // Reset any contacts stuck in 'processing' for more than 5 minutes
  // (can happen if the server crashed mid-send).
  const stuckCutoff = new Date(Date.now() - 5 * 60 * 1000).toISOString();
  await supabaseFetch(
    `/owner_contacts?assigned_agent=eq.${agentId}&message_status=eq.processing&created_at=lt.${stuckCutoff}`,
    {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({ message_status: 'pending' })
    }
  ).catch(e => console.error(`Failed to reset stuck contacts for agent ${agentId}:`, e.message));

  const todayUAE = new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Dubai' });

  // Check daily cap
  const countRes = await supabaseFetch(
    `/messages_log?agent_id=eq.${agentId}&sent_at=gte.${todayUAE}T00:00:00%2B04:00&select=id`,
    { headers: { 'Prefer': 'count=exact' } }
  );
  const range = countRes.headers.get('content-range');
  const sentToday = range ? parseInt(range.split('/')[1]) : 0;
  if (sentToday >= 50) {
    console.log(`Agent ${agentId} hit daily cap of 50`);
    return;
  }

  // Check if agent has paused sending
  const profileRes = await supabaseFetch(`/profiles?id=eq.${agentId}&select=sending_paused`);
  const profiles = await profileRes.json();
  if (profiles.length > 0 && profiles[0].sending_paused === true) {
    console.log(`Agent ${agentId} has paused sending, skipping`);
    return;
  }

  // Time gap between messages
  // Base window: 5–15 min (300–900 s). 20% of the time a longer spike: 15–30 min (900–1800 s).
  if (!TEST_MODE) {
    const lastRes = await supabaseFetch(
      `/messages_log?agent_id=eq.${agentId}&order=sent_at.desc&limit=1&select=sent_at`
    );
    const lastLogs = await lastRes.json();
    if (lastLogs.length > 0) {
      const secondsSinceLast = (Date.now() - new Date(lastLogs[0].sent_at).getTime()) / 1000;
      const spike = Math.random() < 0.20;
      const randomWait = spike
        ? 900  + Math.floor(Math.random() * 900)   // 15–30 min spike
        : 300  + Math.floor(Math.random() * 600);  // 5–15 min standard
      if (secondsSinceLast < randomWait) return;
    }
  }

  // Get next pending contact
  const contactRes = await supabaseFetch(
    `/owner_contacts?assigned_agent=eq.${agentId}&message_status=eq.pending&order=created_at.asc&limit=1`
  );
  const contacts = await contactRes.json();
  if (!contacts.length) return;

  const contact = contacts[0];

  // Claim the contact immediately to prevent two overlapping ticks from both
  // picking it up as pending (race condition guard).
  await supabaseFetch(`/owner_contacts?id=eq.${contact.id}`, {
    method: 'PATCH',
    headers: { 'Prefer': 'return=minimal' },
    body: JSON.stringify({ message_status: 'processing' })
  });

  // Build all phone number variants to handle format mismatches
  // (+971…, 971…, 0…, 00971…) across both tables.
  const variants = phoneVariants(contact.number_1);
  const orLog     = variants.map(v => `number_used.eq.${encodeURIComponent(v)}`).join(',');
  const orContact = variants.map(v => `number_1.eq.${encodeURIComponent(v)}`).join(',');

  // 1. Check messages_log — catches any number that was ever successfully sent
  const [dupLogRes, dupContactRes] = await Promise.all([
    supabaseFetch(`/messages_log?or=(${orLog})&limit=1&select=id`),
    // 2. Check owner_contacts — catches duplicates within the same upload batch
    //    (same number appears twice; one already sent/processing/duplicate)
    supabaseFetch(
      `/owner_contacts?or=(${orContact})&message_status=in.(sent,processing,duplicate)&id=neq.${contact.id}&limit=1&select=id`
    ),
  ]);

  const dupLogs     = await dupLogRes.json();
  const dupContacts = await dupContactRes.json();

  if (dupLogs.length > 0 || dupContacts.length > 0) {
    const source = dupLogs.length > 0 ? 'messages_log' : 'owner_contacts';
    console.log(`Duplicate number ${contact.number_1} found in ${source} — skipping contact ${contact.id}`);
    await supabaseFetch(`/owner_contacts?id=eq.${contact.id}`, {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({ message_status: 'duplicate' })
    });
    return;
  }

  try {
    await sessionManager.sendMessage(agentId, contact.number_1, contact.generated_message);

    const now = new Date().toISOString();

    await supabaseFetch(`/owner_contacts?id=eq.${contact.id}`, {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({ message_status: 'sent', sent_at: now })
    });

    await supabaseFetch('/messages_log', {
      method: 'POST',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({
        contact_id: contact.id,
        agent_id: agentId,
        number_used: contact.number_1,
        message_text: contact.generated_message,
        delivery_status: 'sent',
        sent_at: now
      })
    });

    await supabaseFetch('/rpc/increment_batch_sent', {
      method: 'POST',
      body: JSON.stringify({ batch_id: contact.uploaded_batch_id })
    });

    console.log(`${TEST_MODE ? '[TEST MODE] ' : ''}Sent to ${contact.number_1} for agent ${agentId}`);
  } catch (err) {
    console.error(`Send failed for contact ${contact.id}:`, err.message);
    await supabaseFetch(`/owner_contacts?id=eq.${contact.id}`, {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({ message_status: 'failed' })
    }).catch(e => console.error('Failed to mark contact as failed:', e.message));
  }
}

function startScheduler() {
  if (TEST_MODE) {
    console.log('Scheduler running in TEST MODE — no time restrictions, no gaps between messages');
  } else {
    console.log('Scheduler running in PRODUCTION MODE — 9am to 9pm UAE, 5–15 min gaps (20% chance 15–30 min spike)');
  }
  setInterval(tick, 60000);
}

module.exports = { startScheduler, tick };
