'use strict';

const sessionManager = require('./sessionManager');
const { varyMessage } = require('./services/aiVariation');

// SET THIS TO true FOR TESTING, false FOR PRODUCTION
const TEST_MODE = false;

const FETCH_TIMEOUT_MS = 10000;

const DEFAULT_DAILY_CAP = 25;

function randomGapSeconds(uaeHour) {
  const isLunch = uaeHour === 13;
  const roll = Math.random();

  let bucket;
  if (isLunch) {
    bucket = roll < 0.65 ? 'drifted' : roll < 0.90 ? 'normal' : 'focused';
  } else {
    bucket = roll < 0.40 ? 'focused' : roll < 0.85 ? 'normal' : 'drifted';
  }

  switch (bucket) {
    case 'focused': return 240  + Math.floor(Math.random() * 360);
    case 'normal':  return 600  + Math.floor(Math.random() * 720);
    case 'drifted': return 1680 + Math.floor(Math.random() * 1620);
    default:        return 600;
  }
}

function getEffectiveDailyCap(accountCreatedAt) {
  if (!accountCreatedAt) return DEFAULT_DAILY_CAP;
  const ageMs = Date.now() - new Date(accountCreatedAt).getTime();
  const ageDays = ageMs / (1000 * 60 * 60 * 24);
  if (ageDays <  7) return 15;
  if (ageDays < 14) return 18;
  if (ageDays < 21) return 21;
  if (ageDays < 30) return 23;
  return DEFAULT_DAILY_CAP;
}

function normalizePhone(num) {
  return String(num || '').replace(/\D/g, '');
}

function phoneVariants(num) {
  const digits = normalizePhone(num);
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

async function supabaseFetch(path, options) {
  options = options || {};
  const baseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;

  if (!baseUrl) throw new Error('SUPABASE_URL env var is not set');
  if (!serviceKey) throw new Error('SUPABASE_SERVICE_KEY env var is not set');

  const controller = new AbortController();
  const timer = setTimeout(function() { controller.abort(); }, FETCH_TIMEOUT_MS);

  try {
    const res = await fetch(baseUrl + '/rest/v1' + path, Object.assign({}, options, {
      signal: controller.signal,
      headers: Object.assign({
        'apikey': serviceKey,
        'Authorization': 'Bearer ' + serviceKey,
        'Content-Type': 'application/json',
      }, options.headers || {})
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

// Per-agent in-memory lock. If a previous tick's processAgent for an agent is
// still running (e.g. a slow Green API call or 18 s typing simulation), we
// must NOT start another for the same agent — otherwise the older claim can
// be reset to pending by the stuck-cutoff logic in the second processAgent
// and the SAME contact gets sent twice.
const _agentLocks = new Set();

async function tick() {
  try {
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
  } catch (e) {
    // Hardened: a top-level throw here would otherwise become an unhandled
    // rejection. setInterval keeps firing regardless, but we still log.
    console.error('[scheduler/tick] Unhandled error:', e.message);
  }
}

async function processAgent(agentId) {
  // Skip if a previous tick is still running for this agent. Prevents a slow
  // send (typing sim + network) from being clobbered by the next tick's
  // stuck-cutoff reset, which can otherwise cause double-sends.
  if (_agentLocks.has(agentId)) {
    return;
  }
  _agentLocks.add(agentId);
  try {
    return await _processAgentInner(agentId);
  } finally {
    _agentLocks.delete(agentId);
  }
}

async function _processAgentInner(agentId) {
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

  const profileRes = await supabaseFetch(
    '/profiles?id=eq.' + agentId + '&select=sending_paused,created_at,whatsapp_session_status'
  );
  const profiles = await profileRes.json();
  if (!profiles.length) return;
  const profile = profiles[0];

  // Hard stop — if WhatsApp is not connected at the moment we're about to send,
  // don't claim any contact or attempt any send. The health check or webhook will
  // flip the status back to connected once the agent rescans their QR code.
  if (profile.whatsapp_session_status !== 'connected') {
    console.log('Agent ' + agentId + ' WhatsApp is ' + (profile.whatsapp_session_status || 'unknown') + ' — skipping send');
    return;
  }

  if (profile.sending_paused === true) {
    console.log('Agent ' + agentId + ' has paused sending, skipping');
    return;
  }

  const dailyCap = getEffectiveDailyCap(profile.created_at);

  const countRes = await supabaseFetch(
    '/messages_log?agent_id=eq.' + agentId + '&sent_at=gte.' + todayUAE + 'T00:00:00%2B04:00&select=id',
    { headers: { 'Prefer': 'count=exact' } }
  );
  const range = countRes.headers.get('content-range');
  const sentToday = range ? parseInt(range.split('/')[1]) : 0;
  if (sentToday >= dailyCap) {
    console.log('Agent ' + agentId + ' hit daily cap of ' + dailyCap);
    return;
  }

  if (!TEST_MODE) {
    const lastRes = await supabaseFetch(
      '/messages_log?agent_id=eq.' + agentId + '&order=sent_at.desc&limit=1&select=sent_at'
    );
    const lastLogs = await lastRes.json();
    if (lastLogs.length > 0) {
      const secondsSinceLast = (Date.now() - new Date(lastLogs[0].sent_at).getTime()) / 1000;
      const uaeTime = new Date().toLocaleString('en-US', { timeZone: 'Asia/Dubai' });
      const uaeHour = new Date(uaeTime).getHours();
      const requiredGap = randomGapSeconds(uaeHour);
      if (secondsSinceLast < requiredGap) {
        const remainMin = Math.round((requiredGap - secondsSinceLast) / 60);
        console.log('Agent ' + agentId + ': ' + Math.round(secondsSinceLast / 60) + 'm since last send, need ' + Math.round(requiredGap / 60) + 'm -- waiting ~' + remainMin + 'm');
        return;
      }
    }
  }

  const contactRes = await supabaseFetch(
    '/owner_contacts?assigned_agent=eq.' + agentId + '&message_status=eq.pending&order=created_at.asc&limit=1'
  );
  const contacts = await contactRes.json();
  if (!contacts.length) return;

  const contact = contacts[0];

  // Conditional claim: only flip pending -> processing. If another worker
  // (or a stale tick) already claimed it, the PATCH affects 0 rows and we
  // bail out instead of double-sending.
  const claimRes = await supabaseFetch(
    '/owner_contacts?id=eq.' + contact.id + '&message_status=eq.pending',
    {
      method: 'PATCH',
      headers: { 'Prefer': 'return=representation' },
      body: JSON.stringify({ message_status: 'processing' })
    }
  );
  const claimed = await claimRes.json();
  if (!Array.isArray(claimed) || claimed.length === 0) {
    console.log('[scheduler] Contact ' + contact.id + ' was claimed by another worker — skipping');
    return;
  }

  const variants    = phoneVariants(contact.number_1);
  const orLog       = variants.map(function(v) { return 'number_used.eq.' + encodeURIComponent(v); }).join(',');
  const orContact   = variants.map(function(v) { return 'number_1.eq.'   + encodeURIComponent(v); }).join(',');

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
    console.log('Opted-out number ' + contact.number_1 + ' -- suppressing contact ' + contact.id);
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
    const finalMessage = await varyMessage(contact.generated_message);

    var sendResult;
    // Plain WhatsApp message. Recipients reply by tapping numbered options or
    // free text; webhook.js intent detection (sell/rent/market/stop) routes
    // the reply to the correct status. No poll = no Green API char-limit cap
    // and a much less spam-flagged delivery profile.
    sendResult = await sessionManager.sendMessage(agentId, contact.number_1, finalMessage);

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
        message_text:    finalMessage,
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

    if (/unauthorized|not.?authorized|401/i.test(err.message)) {
      await supabaseFetch('/profiles?id=eq.' + agentId, {
        method: 'PATCH',
        headers: { 'Prefer': 'return=minimal' },
        body: JSON.stringify({ whatsapp_session_status: 'disconnected' })
      }).catch(function(e) { console.error('Failed to mark agent ' + agentId + ' disconnected:', e.message); });
      console.warn('[scheduler] Agent ' + agentId + ' marked disconnected -- Green API returned unauthorized');
    }

    const failNow = new Date().toISOString();
    await supabaseFetch('/owner_contacts?id=eq.' + contact.id, {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({
        message_status: 'failed',
        error_message: String(err.message || err).slice(0, 1000),
        last_error_at: failNow,
        attempts:      ((contact.attempts || 0) + 1)
      })
    }).catch(function(e) { console.error('Failed to mark contact as failed:', e.message); });

    // Also write a row into messages_log so failures are visible alongside successes.
    await supabaseFetch('/messages_log', {
      method: 'POST',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({
        contact_id:      contact.id,
        agent_id:        agentId,
        number_used:     contact.number_1,
        message_text:    null,
        delivery_status: 'failed',
        sent_at:         failNow
      })
    }).catch(function(e) { console.error('Failed to log failed send:', e.message); });
  }
}

// Runs every 5 minutes for ALL active agents (not just those marked connected).
// Reads the real Green API state and reconciles the DB with reality:
//
//   authorized                 → connected
//   notAuthorized | blocked    → disconnected (genuine — needs QR re-scan)
//   starting | sleepMode |     → leave DB unchanged (transient — Green API
//   yellowCard | other            instance is recovering or rate-limited; not
//                                 a real disconnect)
//
// Why we do not flip on every non-'authorized' state: previously a single
// 'starting' (instance boot) reply caused the whole team to be marked
// disconnected and skipped by the scheduler for a full day. Real disconnects
// are notAuthorized (QR expired) or blocked (account banned).
async function syncInstanceStatuses() {
  try {
    const res = await supabaseFetch(
      '/profiles?is_active=eq.true&role=in.(agent,super_admin)' +
      '&green_api_instance_id=not.is.null' +
      '&select=id,first_name,whatsapp_session_status,green_api_instance_id,green_api_token,green_api_url'
    );
    const agents = await res.json();
    if (!Array.isArray(agents) || agents.length === 0) return;

    console.log('[health] Checking ' + agents.length + ' instance(s)...');

    await Promise.all(agents.map(async function(agent) {
      if (!agent.green_api_instance_id || !agent.green_api_token || !agent.green_api_url) return;
      try {
        const controller = new AbortController();
        const timer = setTimeout(function() { controller.abort(); }, 8000);
        const stateRes = await fetch(
          agent.green_api_url + '/waInstance' + agent.green_api_instance_id + '/getStateInstance/' + agent.green_api_token,
          { signal: controller.signal }
        );
        clearTimeout(timer);
        if (!stateRes.ok) throw new Error('HTTP ' + stateRes.status);
        const data = await stateRes.json();
        const state = data.stateInstance;

        let desired = null;
        if (state === 'authorized') {
          desired = 'connected';
        } else if (state === 'notAuthorized' || state === 'blocked') {
          desired = 'disconnected';
        }
        // Any other state (starting, sleepMode, yellowCard, undefined) is
        // treated as transient — leave DB unchanged.

        if (desired === null) {
          console.log('[health] ' + (agent.first_name || agent.id) + ' is ' + state + ' -- transient, leaving status unchanged');
          return;
        }

        if (desired !== agent.whatsapp_session_status) {
          await supabaseFetch('/profiles?id=eq.' + agent.id, {
            method: 'PATCH',
            headers: { 'Prefer': 'return=minimal' },
            body: JSON.stringify({ whatsapp_session_status: desired })
          });
          console.log('[health] ' + (agent.first_name || agent.id) + ' is ' + state + ' -- ' +
            agent.whatsapp_session_status + ' -> ' + desired);
        }
      } catch (e) {
        // Network / timeout — do NOT change status on a transient failure.
        console.warn('[health] Could not check instance ' + agent.green_api_instance_id + ': ' + e.message);
      }
    }));
  } catch (e) {
    console.error('[health] syncInstanceStatuses error:', e.message);
  }
}

function startScheduler() {
  if (TEST_MODE) {
    console.log('Scheduler: TEST MODE -- no time restrictions or gaps');
  } else {
    console.log(
      'Scheduler: PRODUCTION MODE\n' +
      '  Hours:     09:00-21:00 UAE\n' +
      '  Daily cap: ' + DEFAULT_DAILY_CAP + ' (new accounts ramp up over 30 days)\n' +
      '  Gaps:      4-10 min (focused) | 10-22 min (normal) | 28-55 min (drifted) | lunch-aware\n' +
      '  Health:    Green API status sync every 5 min'
    );
  }

  setInterval(tick, 60000);

  // Run health check immediately on startup, then every 5 min
  syncInstanceStatuses();
  setTimeout(function() {
    setInterval(syncInstanceStatuses, 5 * 60 * 1000);
  }, 30000);
}

module.exports = { startScheduler, tick };
