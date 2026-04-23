'use strict';

const express = require('express');
const router  = express.Router();

const FETCH_TIMEOUT_MS  = 10000;
const GEMINI_TIMEOUT_MS = 18000;

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
        'apikey':        serviceKey,
        'Authorization': 'Bearer ' + serviceKey,
        'Content-Type':  'application/json',
      }, options.headers || {}),
    }));
    if (!res.ok) {
      const body = await res.text();
      throw new Error('Supabase ' + (options.method || 'GET') + ' ' + path + ' [' + res.status + ']: ' + body);
    }
    return res;
  } finally {
    clearTimeout(timer);
  }
}

async function sendViaGreenApi(agentCreds, toNumber, message) {
  const chatId = toNumber.replace(/\D/g, '') + '@c.us';
  try {
    const res = await fetch(
      agentCreds.green_api_url + '/waInstance' + agentCreds.green_api_instance_id + '/sendMessage/' + agentCreds.green_api_token,
      { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chatId: chatId, message: message }) }
    );
    if (!res.ok) {
      const body = await res.text();
      console.error('[webhook/send] Green API error: ' + body.slice(0, 200));
      return false;
    }
    return true;
  } catch (e) {
    console.error('[webhook/send] Error:', e.message);
    return false;
  }
}

async function generateReply(originalMessage, leadReply, agentFirstName, geminiKey) {
  try {
    const controller = new AbortController();
    const timeoutId  = setTimeout(function() { controller.abort(); }, GEMINI_TIMEOUT_MS);
    const prompt = 'You are ' + agentFirstName + ', a real estate agent at EVA Real Estate in Dubai.\n\n' +
      'You sent this WhatsApp outreach message to a property owner:\n"""\n' + originalMessage + '\n"""\n\n' +
      'The property owner replied:\n"""\n' + leadReply + '\n"""\n\n' +
      'Write a short (2-4 sentence), warm, natural follow-up reply that:\n' +
      '- Acknowledges specifically what they said\n' +
      '- Moves the conversation forward (suggest a quick call or ask one relevant question)\n' +
      '- Sounds like a real person, not a bot -- conversational, not formal\n' +
      '- Does NOT use hollow phrases like "Great to hear from you!" or "I hope this finds you well"\n\n' +
      'Return only the reply text, no commentary.';
    const res = await fetch(
      'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=' + geminiKey,
      { method: 'POST', signal: controller.signal, headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }], generationConfig: { temperature: 1.0 } }) }
    );
    clearTimeout(timeoutId);
    if (!res.ok) { console.warn('[webhook/Gemini] HTTP ' + res.status); return null; }
    const data = await res.json();
    const txt  = data && data.candidates && data.candidates[0] && data.candidates[0].content &&
                 data.candidates[0].content.parts && data.candidates[0].content.parts[0] &&
                 data.candidates[0].content.parts[0].text;
    return txt ? txt.trim() : null;
  } catch (e) {
    console.warn('[webhook/Gemini] Error:', e.message);
    return null;
  }
}

const STOP_PATTERNS = [/\bstop\b/i, /\bunsubscribe\b/i, /\bremove\b/i, /\bopt.?out\b/i,
                       /\bnot interested\b/i, /\bno thanks\b/i, /\bno thank you\b/i];
const RENT_PATTERNS = [/\brent\b/i, /^1$/];
const SELL_PATTERNS = [/\bsell\b/i, /\bsale\b/i, /^2$/];

function detectIntent(text) {
  const t = (text || '').trim();
  if (STOP_PATTERNS.some(function(r) { return r.test(t); })) return 'stop';
  if (RENT_PATTERNS.some(function(r) { return r.test(t); })) return 'rent';
  if (SELL_PATTERNS.some(function(r) { return r.test(t); })) return 'sell';
  return 'conversation';
}

// ── Button reply handler ──────────────────────────────────────────────────────
// Handles `buttonsResponseMessage` webhook events from Green API.
// Triggered when a recipient taps one of the reply buttons.

async function handleButtonReply(payload, fromNumber, contact) {
  var btnData = payload && payload.messageData && payload.messageData.buttonsResponseMessage
    ? payload.messageData.buttonsResponseMessage : null;
  if (!btnData) return;

  // Green API sends selectedButtonId and selectedButtonText
  var buttonId   = (btnData.selectedButtonId   || '').toLowerCase();
  var buttonText = (btnData.selectedButtonText || '').toLowerCase();
  console.log('[webhook/btn] ' + fromNumber + ' tapped: "' + buttonId + '" / "' + buttonText + '"');

  var newStatus;
  if (buttonId === 'btn_rent'   || buttonText.includes('rent')) {
    newStatus = 'interested_rent';
  } else if (buttonId === 'btn_sell' || buttonText.includes('sell')) {
    newStatus = 'interested_sell';
  } else if (buttonId === 'btn_not_interested' || buttonText.includes('not interested')) {
    newStatus = 'opted_out';
  } else {
    // Unknown button -- treat as a general reply
    newStatus = 'replied';
  }

  var now           = new Date().toISOString();
  var newReplyCount = ((contact.reply_count) || 0) + 1;

  await supabaseFetch('/owner_contacts?id=eq.' + contact.id, {
    method: 'PATCH', headers: { 'Prefer': 'return=minimal' },
    body: JSON.stringify({ message_status: newStatus, reply_count: newReplyCount, replied_at: now })
  });

  var agentRes     = await supabaseFetch('/profiles?id=eq.' + contact.assigned_agent +
    '&select=first_name,green_api_url,green_api_instance_id,green_api_token');
  var agentRows    = await agentRes.json();
  var agentProfile = agentRows[0];
  if (!agentProfile || !agentProfile.green_api_instance_id) return;

  if (newStatus === 'opted_out') {
    await sendViaGreenApi(agentProfile, fromNumber,
      'No problem at all -- you have been removed and will not hear from us again. Have a great day!');
    console.log('[webhook/btn] ' + fromNumber + ' opted out via button');
    return;
  }

  // Rent or sell -- generate a contextual follow-up via Gemini
  var settingsRes  = await supabaseFetch('/api_settings?id=eq.1&select=gemini_api_key');
  var settingsRows = await settingsRes.json();
  var geminiKey    = settingsRows[0] && settingsRows[0].gemini_api_key ? settingsRows[0].gemini_api_key : '';

  var intent     = newStatus === 'interested_rent' ? 'rent' : 'sell';
  var votedLabel = intent === 'rent' ? 'rent it out' : 'sell it';
  var replyText  = geminiKey
    ? await generateReply(contact.generated_message, 'I want to ' + votedLabel, agentProfile.first_name, geminiKey)
    : null;
  var fallback = intent === 'rent'
    ? 'Thanks for letting me know! I would love to help you get it rented. When would be a good time for a quick 5-minute call?'
    : 'The market is strong right now. Can we jump on a quick call this week? I can walk you through what your unit could realistically achieve.';

  await sendViaGreenApi(agentProfile, fromNumber, replyText || fallback);
  console.log('[webhook/btn] Sent ' + intent + ' follow-up to ' + fromNumber);
}

// ── Poll vote handler (legacy -- handles votes on messages sent before the switch) ──
// Kept for backward compatibility with any poll messages already sent.



async function handleTextReply(fromNumber, messageText, contact) {
  var intent        = detectIntent(messageText);
  var now           = new Date().toISOString();
  var newReplyCount = ((contact.reply_count) || 0) + 1;
  console.log('[webhook/text] ' + fromNumber + ' replied -- intent: ' + intent);
  var newStatus;
  if (intent === 'stop')  newStatus = 'opted_out';
  else if (intent === 'rent') newStatus = 'interested_rent';
  else if (intent === 'sell') newStatus = 'interested_sell';
  else                        newStatus = 'replied';

  await supabaseFetch('/owner_contacts?id=eq.' + contact.id, {
    method: 'PATCH', headers: { 'Prefer': 'return=minimal' },
    body: JSON.stringify({ message_status: newStatus, reply_count: newReplyCount, replied_at: now })
  });

  var agentRes     = await supabaseFetch('/profiles?id=eq.' + contact.assigned_agent +
    '&select=first_name,green_api_url,green_api_instance_id,green_api_token');
  var agentRows    = await agentRes.json();
  var agentProfile = agentRows[0];
  if (!agentProfile || !agentProfile.green_api_instance_id) return;

  if (intent === 'stop') {
    await sendViaGreenApi(agentProfile, fromNumber,
      'No problem -- you have been removed from our list and will not hear from us again. Have a great day!');
    return;
  }

  var settingsRes  = await supabaseFetch('/api_settings?id=eq.1&select=gemini_api_key');
  var settingsRows = await settingsRes.json();
  var geminiKey    = settingsRows[0] && settingsRows[0].gemini_api_key ? settingsRows[0].gemini_api_key : '';
  if (!geminiKey) return;

  var replyText = await generateReply(contact.generated_message, messageText, agentProfile.first_name, geminiKey);
  if (replyText) {
    await sendViaGreenApi(agentProfile, fromNumber, replyText);
    console.log('[webhook/text] Gemini reply sent to ' + fromNumber + ' (intent: ' + intent + ')');
  }
}

// ── Incoming webhook route ────────────────────────────────────────────────────

router.post('/incoming', async function(req, res) {
  res.sendStatus(200);
  try {
    var payload = req.body;
    if (payload.typeWebhook !== 'incomingMessageReceived') return;

    var rawSender  = (payload.senderData && (payload.senderData.sender || payload.senderData.chatId)) || '';
    var fromNumber = rawSender.replace('@c.us', '').replace(/\D/g, '');
    if (!fromNumber) return;

    var msgType       = (payload.messageData && payload.messageData.typeMessage) || '';
    var isButtonReply = msgType === 'buttonsResponseMessage';

    var messageText = (
      (payload.messageData && payload.messageData.textMessageData && payload.messageData.textMessageData.textMessage) ||
      (payload.messageData && payload.messageData.extendedTextMessageData && payload.messageData.extendedTextMessageData.text) ||
      ''
    ).trim();

    var lookupRes = await supabaseFetch(
      '/owner_contacts?number_1=eq.' + encodeURIComponent(fromNumber) +
      '&message_status=in.(sent,replied,interested_rent,interested_sell)&order=sent_at.desc&limit=1' +
      '&select=id,reply_count,generated_message,assigned_agent,building_name'
    );
    var contacts = await lookupRes.json();
    var contact  = Array.isArray(contacts) && contacts.length > 0 ? contacts[0] : null;

    await supabaseFetch('/incoming_messages', {
      method: 'POST',
      body: JSON.stringify({
        contact_id:   contact ? contact.id : null,
        from_number:  fromNumber,
        message_text: isButtonReply ? '[BUTTON REPLY] ' + msgType : messageText,
        raw_payload:  payload,
        matched:      !!contact,
      }),
    }).catch(function(e) { console.error('[webhook] Log error:', e.message); });

    if (!contact) return;

    if (isButtonReply)       await handleButtonReply(payload, fromNumber, contact);
    else if (messageText) await handleTextReply(fromNumber, messageText, contact);
  } catch (err) {
    console.error('[webhook] Unhandled error:', err.message);
  }
});

module.exports = router;
