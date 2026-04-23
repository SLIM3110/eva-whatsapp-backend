const sessionManager = require('./sessionManager');

// SET THIS TO true FOR TESTING, false FOR PRODUCTION
const TEST_MODE = false;

const FETCH_TIMEOUT_MS = 10000;

// ── Daily send cap ────────────────────────────────────────────────────────────
const DEFAULT_DAILY_CAP = 25;

// ── Timing strategy ───────────────────────────────────────────────────────────
function randomGapSeconds(uaeHour) {
  const isLunch = uaeHour === 13;
  const roll = Math.random();

  var bucket;
  if (isLunch) {
    bucket = roll < 0.65 ? 'drifted' : roll < 0.90 ? 'normal' : 'focused';
  } else {
    bucket = roll < 0.40 ? 'focused' : roll < 0.85 ? 'normal' : 'drifted';
  }

  switch (bucket) {
    case 'focused': return 240  + Math.floor(Math.random() * 360);   // 4-10 min
    case 'normal':  return 600  + Math.floor(Math.random() * 720);   // 10-22 min
    case 'drifted': return 1680 + Math.floor(Math.random() * 1620);  // 28-55 min
    default:        return 600;
  }
}

// ── Account warmup cap ────────────────────────────────────────────────────────
function getEffectiveDailyCap(accountCreatedAt) {
  if (!accountCreatedAt) return DEFAULT_DAILY_CAP;
  const ageMs   = Date.now() - new Date(accountCreatedAt).getTime();
  const ageDays = ageMs / (1000 * 60 * 60 * 24);
  if (ageDays <  7) return 15;
  if (ageDays < 14) return 18;
  if (ageDays < 21) return 21;
  if (ageDays < 30) return 23;
  return DEFAULT_DAILY_CAP;
}

// ── Phone normalisation ───────────────────────────────────────────────────────

function normalizePhone(num) {
  return String(num || '').replace(/\D/g, '');
}

function phoneVariants(num) {
  const digits   = normalizePhone(num);
  const variants = new Set([digits, '+' + digits]);
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

// ── Supabase fetch wrapper ────────────────────────────────────────────────────

async function supabaseFetch(path, options) {
  options = options || {};
  const baseUrl    = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;

  const controller = new AbortController();
  const timer      = setTimeout(function() { controller.abort(); }, FETCH_TIMEOUT_MS);

  try {
    const res = await fetch(baseUrl + '/rest/v1' + path, Object.assign({}, options, {
      signal: controller.signal,
      headers: Object.assign({
        'apikey':          serviceKey,
        'Authorization':   'Bearer ' + serviceKey,
        'Content-Type':    'application/json',
      }, options.headers || {}),
    }));
    if (!res.ok) {
      const body = await res.text();
      throw new Error('Supabase ' + (options.method || 'GET') + ' ' + path + ' failed [' + res.status + ']: ' + body);
    }
    return res;
  } finally {
    clearTimeout(timer);
  }
}

// ── Main tick ─────────────────────────────────────────────────────────────────

async function tick() {
  if (!TEST_MODE) {
    const uaeTime = new Date().toLocaleString('en-US', { timeZone: 'Asia/Dubai' });
    const uaeHour = new Date(uaeTime).getHours();
    if (uaeHour < 9 || uaeHour >= 21) {
      console.log('Scheduler: outside UAE business hours (' + uaeHour + ':00), skipping');
      return;
    }
  }

  const agentsRes = await supabaseFetch(
    '/profiles?whatsapp_session_status=eq.connected&is_active=eq.true&role=in.(agent,super_admin)&select=id'
  );
  const agents = await agentsRes.json();
  if (!Array.isArray(agents)) return;

  await Promise.all(
    agents.map(function(agent) {
      return processAgent(agent.id).catch(function(e) {
        console.error('Scheduler error for agent ' + agent.id + ':', e.message);
      });
    })
  );
}

// ── Per-agent processing ──────────────────────────────────────────────────────

async function processAgent(agentId) {
  // Reset contacts stuck in 'processing' for > 10 min (crash recovery)
  const stuckCutoff = new Date(Date.now() - 10 * 60 * 1000).toISOString();
  await supabaseFetch(
    '/owner_contacts?assigned_agent=eq.' + agentId + '&message_status=eq.processing&created_at=lt.' + stuckCutoff,
    {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({ message_status: 'pending' })
    }
  ).catch(function(e) { console.error('Failed to reset stuck contacts for agent ' + agentId + ':', e.message); });

  const todayUAE = new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Dubai' });

  // Fetch profile for pause flag + account creation date (for warmup cap)
  const profileRes = await supabaseFetch('/profiles?id=eq.' + agentId + '&select=sending_paused,created_at');
  const profiles   = await profileRes.json();
  if (!profiles.length) return;
  const profile = profiles[0];

  if (profile.sending_paused === true) {
    console.log('Agent ' + agentId + ' has paused sending, skipping');
    return;
  }

  // Effective daily cap -- respects account warmup schedule
  const dailyCap = getEffectiveDailyCap(profile.created_at);

  // Check daily send count
  const countRes  = await supabaseFetch(
    '/messages_log?agent_id=eq.' + agentId + '&sent_at=gte.' + todayUAE + 'T00:00:00%2B04:00&select=id',
    { headers: { 'Prefer': 'count=exact' } }
  );
  const range     = countRes.headers.get('content-range');
  const sentToday = range ? parseInt(range.split('/')[1]) : 0;
  if (sentToday >= dailyCap) {
    console.log('Agent ' + agentId + ' hit daily cap of ' + dailyCap);
    return;
  }

  // Enforce minimum time gap since last send
  if (!TEST_MODE) {
    const lastRes  = await supabaseFetch(
      '/messages_log?agent_id=eq.' + agentId + '&order=sent_at.desc&limit=1&select=sent_at'
    );
    const lastLogs = await lastRes.json();
    if (lastLogs.length > 0) {
      const secondsSinceLast = (Date.now() - new Date(lastLogs[0].sent_at).getTime()) / 1000;
      const uaeTime    = new Date().toLocaleString('en-US', { timeZone: 'Asia/Dubai' });
      const uaeHour    = new Date(uaeTime).getHours();
      const requiredGap = randomGapSeconds(uaeHour);

      if (secondsSinceLast < requiredGap) {
        const remainMin = Math.round((requiredGap - secondsSinceLast) / 60);
        console.log('Agent ' + agentId + ': ' + Math.round(secondsSinceLast / 60) + 'm since last send, need ' + Math.round(requiredGap / 60) + 'm -- waiting ~' + remainMin + 'm');
        return;
      }
    }
  }

  // Get next pending contact
  const contactRes = await supabaseFetch(
    '/owner_contacts?assigned_agent=eq.' + agentId + '&message_status=eq.pending&order=created_at.asc&limit=1'
  );
  const contacts = await contactRes.json();
  if (!contacts.length) return;

  const contact = contacts[0];

  await supabaseFetch('/owner_contacts?id=eq.' + contact.id, {
    method: 'PATCH',
    headers: { 'Prefer': 'return=minimal' },
    body: JSON.stringify({ message_status: 'processing' })
  });

  const variants    = phoneVariants(contact.number_1);
  const orLog       = variants.map(function(v) { return 'number_used.eq.' + encodeURIComponent(v); }).join(',');
  const orContact   = variants.map(function(v) { return 'number_1.eq.' + encodeURIComponent(v); }).join(',');

  const results = await Promise.all([
    supabaseFetch('/messages_log?or=(' + orLog + ')&limit=1&select=id'),
    supabaseFetch(
      '/owner_contacts?or=(' + orContact + ')&message_status=in.(sent,processing,duplicate,replied,interested_rent,interested_sell)&id=neq.' + contact.id + '&limit=1&select=id'
    ),
    supabaseFetch(
      '/owner_contacts?or=(' + orContact + ')&message_status=eq.opted_out&limit=1&select=id'
    ),
  ]);

  const dupLogs     = await results[0].json();
  const dupContacts = await results[1].json();
  const optedOut    = await results[2].json();

  if (optedOut.length > 0) {
    console.log('Opted-out number ' + contact.number_1 + ' -- permanently suppressing contact ' + contact.id);
    await supabaseFetch('/owner_contacts?id=eq.' + contact.id, {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({ message_status: 'opted_out' })
    });
    return;
  }

  if (dupLogs.length > 0 || dupContacts.length > 0) {
    const source = dupLogs.length > 0 ? 'messages_log' : 'owner_contacts';
    console.log('Duplicate ' + contact.number_1 + ' in ' + source + ' -- skipping ' + contact.id);
    await supabaseFetch('/owner_contacts?id=eq.' + contact.id, {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({ message_status: 'duplicate' })
    });
    return;
  }

  try {
    // Send the outreach message.
    // If send_poll is true (the default), attach 3 tap-to-reply buttons (Sell / Rent / Not interested).
    // If send_poll is false, send the message as plain text with no buttons.
    var sendResult;
    if (contact.send_poll !== false) {
      sendResult = await sessionManager.sendButtons(
        agentId,
        contact.number_1,
        contact.generated_message
      );
    } else {
      sendResult = await sessionManager.sendMessage(
        agentId,
        contact.number_1,
        contact.generated_message
      );
    }

    const now = new Date().toISOString();

    await supabaseFetch('/owner_contacts?id=eq.' + contact.id, {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({ message_status: 'sent', sent_at: now })
    });

    await supabaseFetch('/messages_log', {
      method: 'POST',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({
        contact_id:      contact.id,
        agent_id:        agentId,
        number_used:     contact.number_1,
        message_text:    contact.generated_message,
        delivery_status: 'sent',
        sent_at:         now
      })
    });

    await supabaseFetch('/rpc/increment_batch_sent', {
      method: 'POST',
      body: JSON.stringify({ batch_id: contact.uploaded_batch_id })
    });

    console.log((TEST_MODE ? '[TEST] ' : '') + 'Sent to ' + contact.number_1 + ' for agent ' + agentId + ' (' + (sentToday + 1) + '/' + dailyCap + ' today)');
  } catch (err) {
    console.error('Send failed for contact ' + contact.id + ':', err.message);
    await supabaseFetch('/owner_contacts?id=eq.' + contact.id, {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({ message_status: 'failed' })
    }).catch(function(e) { console.error('Failed to mark contact as failed:', e.message); });
  }
}

function startScheduler() {
  if (TEST_MODE) {
    console.log('Scheduler: TEST MODE -- no time restrictions or gaps');
  } else {
    console.log(
      'Scheduler: PRODUCTION MODE\n' +
      '  Hours:    09:00-21:00 UAE\n' +
      '  Daily cap: ' + DEFAULT_DAILY_CAP + ' (new accounts ramp up over 30 days)\n' +
      '  Gaps:     4-10 min (focused) | 10-22 min (normal) | 28-55 min (drifted) | lunch-aware'
    );
  }
  setInterval(tick, 60000);
}

module.exports = { startScheduler, tick };
