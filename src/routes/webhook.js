const express = require('express');
const router = express.Router();

const FETCH_TIMEOUT_MS = 10000;

async function supabaseFetch(path, options = {}) {
  const baseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;

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

router.post('/incoming', async (req, res) => {
  try {
    const payload = req.body;

    if (payload.typeWebhook !== 'incomingMessageReceived') {
      return res.sendStatus(200);
    }

    const rawSender = payload?.senderData?.sender || payload?.senderData?.chatId || '';
    const fromNumber = rawSender.replace('@c.us', '').replace(/\D/g, '');
    const messageText = payload?.messageData?.textMessageData?.textMessage || '';

    // Look up matching contact
    const lookupRes = await supabaseFetch(
      `/owner_contacts?number_1=eq.${encodeURIComponent(fromNumber)}&message_status=eq.sent&select=id&limit=1`
    );
    const contacts = await lookupRes.json();
    const contact = Array.isArray(contacts) && contacts.length > 0 ? contacts[0] : null;

    if (contact) {
      // Fetch current reply_count then increment
      const contactRes = await supabaseFetch(
        `/owner_contacts?id=eq.${contact.id}&select=reply_count`
      );
      const [contactData] = await contactRes.json();
      const newCount = ((contactData && contactData.reply_count) || 0) + 1;

      await supabaseFetch(`/owner_contacts?id=eq.${contact.id}`, {
        method: 'PATCH',
        headers: { 'Prefer': 'return=minimal' },
        body: JSON.stringify({
          reply_count: newCount,
          replied_at: new Date().toISOString()
        })
      });

      // Insert incoming message with matched = true
      await supabaseFetch('/incoming_messages', {
        method: 'POST',
        body: JSON.stringify({
          contact_id: contact.id,
          from_number: fromNumber,
          message_text: messageText,
          raw_payload: payload,
          matched: true
        })
      });
    } else {
      // Insert incoming message with matched = false
      await supabaseFetch('/incoming_messages', {
        method: 'POST',
        body: JSON.stringify({
          from_number: fromNumber,
          message_text: messageText,
          raw_payload: payload,
          matched: false
        })
      });
    }

    return res.sendStatus(200);
  } catch (err) {
    console.error('Webhook error:', err.message);
    return res.sendStatus(200);
  }
});

module.exports = router;
