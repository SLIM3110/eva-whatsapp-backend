'use strict';

const express = require('express');
const router  = express.Router();

const FETCH_TIMEOUT_MS = 8000;

async function supabaseFetch(path) {
  const baseUrl    = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;
  const controller = new AbortController();
  const timer      = setTimeout(function() { controller.abort(); }, FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(baseUrl + '/rest/v1' + path, {
      signal: controller.signal,
      headers: {
        apikey:        serviceKey,
        Authorization: 'Bearer ' + serviceKey,
        'Content-Type': 'application/json',
      },
    });
    if (!res.ok) throw new Error('Supabase ' + path + ' [' + res.status + ']');
    return res.json();
  } finally {
    clearTimeout(timer);
  }
}

router.get('/healthz', function(_req, res) {
  res.json({ ok: true, ts: new Date().toISOString() });
});

router.get('/readyz', async function(_req, res) {
  const STALE_HOURS = 4;
  try {
    const lastLog = await supabaseFetch('/messages_log?order=sent_at.desc&limit=1&select=sent_at,delivery_status');
    const last    = Array.isArray(lastLog) && lastLog.length ? lastLog[0] : null;
    const lastTs  = last ? new Date(last.sent_at).getTime() : null;
    const ageHrs  = lastTs ? (Date.now() - lastTs) / 3600000 : null;

    const profiles = await supabaseFetch('/profiles?select=whatsapp_session_status,is_active,role&is_active=eq.true&role=in.(agent,super_admin)');
    const connected = profiles.filter(function(p) { return p.whatsapp_session_status === 'connected'; }).length;
    const total     = profiles.length;

    const queue = await supabaseFetch('/owner_contacts?select=id&message_status=eq.pending&limit=1000');
    const pending = Array.isArray(queue) ? queue.length : 0;

    const uaeNow  = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Dubai' }));
    const uaeHour = uaeNow.getHours();
    const uaeMin  = uaeNow.getMinutes();
    const inHours = uaeHour >= 9 && uaeHour < 21;

    const stalled = inHours && pending > 0 && (ageHrs == null || ageHrs > STALE_HOURS);

    var reason;
    if (!inHours) {
      var resumeHour = 9;
      var sleepHrs = ((resumeHour + 24 - uaeHour) % 24);
      if (sleepHrs === 0 && uaeMin > 0) sleepHrs = 24;
      reason = 'Outside business hours (UAE ' + String(uaeHour).padStart(2,'0') + ':' + String(uaeMin).padStart(2,'0') + '). Sending resumes at 09:00 UAE (~' + sleepHrs + 'h).';
    } else if (pending === 0) {
      reason = 'No pending contacts in queue.';
    } else if (connected === 0) {
      reason = 'No agents connected. ' + pending + ' contacts waiting.';
    } else if (stalled) {
      reason = 'Stalled: ' + connected + ' agent(s) connected, ' + pending + ' pending, but no sends in ' + (ageHrs ? ageHrs.toFixed(1) : '?') + ' hours.';
    } else {
      reason = 'Healthy: ' + connected + ' agent(s) sending, ' + pending + ' pending.';
    }

    var body = {
      ok:                !stalled,
      stalled:           stalled,
      reason:            reason,
      last_send_at:      last ? last.sent_at : null,
      last_send_age_hrs: ageHrs ? Math.round(ageHrs * 10) / 10 : null,
      pending:           pending,
      connected:         connected,
      total_active:      total,
      uae_hour:          uaeHour,
      in_business_hours: inHours,
      ts:                new Date().toISOString(),
    };
    res.status(stalled ? 503 : 200).json(body);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

module.exports = router;
