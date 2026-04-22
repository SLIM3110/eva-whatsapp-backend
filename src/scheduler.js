const sessionManager = require('./sessionManager');

// SET THIS TO true FOR TESTING, false FOR PRODUCTION
const TEST_MODE = false;

const FETCH_TIMEOUT_MS = 10000;

// ── Daily send cap ────────────────────────────────────────────────────────────
// 25/day is meaningfully safer than 50 for account longevity.
// WhatsApp risk systems weigh outbound-only volume heavily.
// Accounts under ~3 months old should be treated as "warming" and will
// automatically get a lower cap (see getEffectiveDailyCap below).
const DEFAULT_DAILY_CAP = 25;

// ── Timing strategy ───────────────────────────────────────────────────────────
// Rather than a single fixed gap range we model three behaviour patterns
// a human salesperson would exhibit across the day:
//
//   "focused"  (40%): back-to-back outreach session → 4–10 min gaps
//   "normal"   (45%): working but distracted        → 10–22 min gaps
//   "drifted"  (15%): meeting / coffee / call       → 28–55 min gaps
//
// Between 13:00–14:00 UAE (lunch) the scheduler picks "drifted" 65% of
// the time, mimicking a natural lunch break.
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
    case 'focused': return 240  + Math.floor(Math.random() * 360);   // 4–10 min
    case 'normal':  return 600  + Math.floor(Math.random() * 720);   // 10–22 min
    case 'drifted': return 1680 + Math.floor(Math.random() * 1620);  // 28–55 min
    default:        return 600;
  }
}

// ── Account warmup cap ────────────────────────────────────────────────────────
// New Green API instances ramp up gradually:
//   Week 1  (days  1–7):  15/day
//   Week 2  (days  8–14): 18/day
//   Week 3  (days 15–21): 21/day
//   Week 4  (days 22–30): 23/day
//   30+ days:             DEFAULT_DAILY_CAP (25)
//
// account_created_at is read from the profile. If absent we use DEFAULT_DAILY_CAP.
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

// ── Phone normalisation ───────────────────────────────────────────────────────

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

// ── Supabase fetch wrapper ────────────────────────────────────────────────────

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

// ── Main tick ─────────────────────────────────────────────────────────────────

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

// ── Per-agent processing ──────────────────────────────────────────────────────

async function processAgent(agentId) {
  // Reset contacts stuck in 'processing' for > 10 min (crash recovery)
  // Extended to 10 min to account for the typing simulation delay (up to 18s)
  // plus any retries, so we don't re-queue a message that's mid-send.
  const stuckCutoff = new Date(Date.now() - 10 * 60 * 1000).toISOString();
  await supabaseFetch(
    `/owner_contacts?assigned_agent=eq.${agentId}&message_status=eq.processing&created_at=lt.${stuckCutoff}`,
    {
      method: 'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body: JSON.stringify({ message_status: 'pending' })
    }
  ).catch(e => console.error(`Failed to reset stuck contacts for agent ${agentId}:`, e.message));

  const todayUAE = new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Dubai' });

  // Fetch profile for pause flag + account creation date (for warmup cap)
  const profileRes = await supabaseFetch(
    `/profiles?id=eq.${agentId}&select=sending_paused,created_at`
  );
  const profiles = await profileRes.json();
  if (!profiles.length) return;
  const profile = profiles[0];

  if (profile.sending_paused === true) {
    console.log(`Agent ${agentId} has paused sending, skipping`);
    return;
  }

  // Effective daily cap — respects account warmup schedule
  const dailyCap = getEffectiveDailyCap(profile.created_at);

  // Check daily send count
  const countRes = await supabaseFetch(
    `/messages_log?agent_id=eq.${agentId}&sent_at=gte.${todayUAE}T00:00:00%2B04:00&select=id`,
    { headers: { 'Prefer': 'count=exact' } }
  );
  const range = countRes.headers.get('content-range');
  const sentToday = range ? parseInt(range.split('/')[1]) : 0;
  if (sentToday >= dailyCap) {
    console.log(`Agent ${agentId} hit daily cap of ${dailyCap}`);
    return;
  }

  // Enforce minimum time gap since last send
  if (!TEST_MODE) {
    const lastRes = await supabaseFetch(
      `/messages_log?agent_id=eq.${agentId}&order=sent_at.desc&limit=1&select=sent_at`
    );
    const lastLogs = await lastRes.json();
    if (lastLogs.length > 0) {
      const secondsSinceLast = (Date.now() - new Date(lastLogs[0].sent_at).getTime()) / 1000;

      // Get current UAE hour for lunch-aware gap selection
      const uaeTime = new Date().toLocaleString('en-US', { timeZone: 'Asia/Dubai' });
      const uaeHour = new Date(uaeTime).getHours();
      const requiredGap = randomGapSeconds(uaeHour);

      if (secondsSinceLast < requiredGap) {
        const remainMin = Math.round((requiredGap - secondsSinceLast) / 60);
        console.log(`Agent ${agentId}: ${Math.round(secondsSinceLast / 60)}m since last send, need ${Math.round(requiredGap / 60)}m — waiting ~${remainMin}m`);
        return;
      }
    }
  }

  // Get next pending contact
  const contactRes = await supabaseFetch(
    `/owner_contacts?assigned_agent=eq.${agentId}&message_status=eq.pending&order=created_at.asc&limit=1`
  );
  const contacts = await contactRe