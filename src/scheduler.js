const sessionManager = require('./sessionManager');

async function supabaseFetch(path, options = {}) {
  const res = await fetch(`${process.env.SUPABASE_URL}/rest/v1${path}`, {
    ...options,
    headers: {
      'apikey': process.env.SUPABASE_SERVICE_KEY,
      'Authorization': `Bearer ${process.env.SUPABASE_SERVICE_KEY}`,
      'Content-Type': 'application/json',
      ...(options.headers || {})
    }
  });
  return res;
}

async function tick() {
  const uaeTime = new Date().toLocaleString('en-US', { timeZone: 'Asia/Dubai' });
  const uaeHour = new Date(uaeTime).getHours();
  if (uaeHour < 9 || uaeHour >= 19) return;

  const agentsRes = await supabaseFetch(
    '/profiles?whatsapp_session_status=eq.connected&is_active=eq.true&select=id'
  );
  const agents = await agentsRes.json();
  if (!Array.isArray(agents)) return;

  for (const agent of agents) {
    try { await processAgent(agent.id); }
    catch (e) { console.error(`Scheduler error for agent ${agent.id}:`, e.message); }
  }
}

async function processAgent(agentId) {
  const todayUAE = new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Dubai' });

  const countRes = await supabaseFetch(
    `/messages_log?agent_id=eq.${agentId}&sent_at=gte.${todayUAE}T00:00:00+04:00&select=id`,
    { headers: { 'Prefer': 'count=exact' } }
  );
  const range = countRes.headers.get('content-range');
  const sentToday = range ? parseInt(range.split('/')[1]) : 0;
  if (sentToday >= 50) return;

  const lastRes = await supabaseFetch(
    `/messages_log?agent_id=eq.${agentId}&order=sent_at.desc&limit=1&select=sent_at`
  );
  const lastLogs = await lastRes.json();
  if (lastLogs.length > 0) {
    const secondsSinceLast = (Date.now() - new Date(lastLogs[0].sent_at).getTime()) / 1000;
    const randomWait = 120 + Math.floor(Math.random() * 180);
    if (secondsSinceLast < randomWait) return;
  }

  const contactRes = await supabaseFetch(
    `/owner_contacts?assigned_agent=eq.${agentId}&message_status=eq.pending&order=created_at.asc&limit=1`
  );
  const contacts = await contactRes.json();
  if (!contacts.length) return;

  const contact = contacts[0];

  try {
    await sessionManager.sendMessage(agentId, contact.number_1, contact.generated_message);

    await supabaseFetch(`/owner_contacts?id=eq.${contact.id}`, {
      method: 'PATCH',
      body: JSON.stringify({ message_status: 'sent' })
    });

    await supabaseFetch('/messages_log', {
      method: 'POST',
      body: JSON.stringify({
        contact_id: contact.id,
        agent_id: agentId,
        number_used: contact.number_1,
        message_text: contact.generated_message,
        delivery_status: 'sent'
      })
    });

    await supabaseFetch('/rpc/increment_batch_sent', {
      method: 'POST',
      body: JSON.stringify({ batch_id: contact.uploaded_batch_id })
    });

    console.log(`Sent to ${contact.number_1} for agent ${agentId}`);
  } catch (err) {
    console.error(`Send failed for contact ${contact.id}:`, err.message);
    await supabaseFetch(`/owner_contacts?id=eq.${contact.id}`, {
      method: 'PATCH',
      body: JSON.stringify({ message_status: 'failed' })
    });
  }
}

function startScheduler() {
  console.log('Scheduler started — ticking every 60 seconds');
  setInterval(tick, 60000);
}

module.exports = { startScheduler };
