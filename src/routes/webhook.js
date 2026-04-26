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
      'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=' + geminiKey,
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

// ── Intent detection ──────────────────────────────────────────────────────────

const STOP_PATTERNS   = [/\bstop\b/i, /\bunsubscribe\b/i, /\bremove\b/i, /\bopt.?out\b/i,
                         /\bnot interested\b/i, /\bno thanks\b/i, /\bno thank you\b/i,
                         /\bلا شكرا\b/i, /\bلا يهمني\b/i];
const RENT_PATTERNS   = [/\brent\b/i, /\bإيجار\b/i, /^1$/, /^١$/];
const SELL_PATTERNS   = [/\bsell\b/i, /\bsale\b/i, /^2$/, /^٢$/];
const MARKET_PATTERNS = [/\bmarket\b/i, /\breport\b/i, /\bmarket data\b/i, /\bmarket update\b/i,
                         /^4$/, /^٤$/, /\bتقرير\b/i];

function detectIntent(text) {
  const t = (text || '').trim();
  if (STOP_PATTERNS.some(function(r) { return r.test(t); }))   return 'stop';
  if (MARKET_PATTERNS.some(function(r) { return r.test(t); })) return 'market';
  if (RENT_PATTERNS.some(function(r) { return r.test(t); }))   return 'rent';
  if (SELL_PATTERNS.some(function(r) { return r.test(t); }))   return 'sell';
  return 'conversation';
}

// ── Poll vote parser ──────────────────────────────────────────────────────────
// Green API delivers poll votes as typeMessage: "pollUpdateMessage".
// The votes array lists each option and which chatIds selected it.

function parsePollVote(payload, fromNumber) {
  const pollData = payload && payload.messageData && payload.messageData.pollMessageData
    ? payload.messageData.pollMessageData : null;
  if (!pollData) return null;

  const voterChatId = fromNumber + '@c.us';
  const votes = pollData.votes || [];
  for (const vote of votes) {
    const voters = vote.optionVoters || [];
    if (voters.some(function(v) { return v === voterChatId || v.replace('@c.us', '') === fromNumber; })) {
      return (vote.optionName || '').toLowerCase();
    }
  }
  return null;
}

// ── Poll vote handler ─────────────────────────────────────────────────────────

async function handlePollVote(payload, fromNumber, contact) {
  const votedOption = parsePollVote(payload, fromNumber);
  if (!votedOption) return;

  console.log('[webhook/poll] ' + fromNumber + ' voted: "' + votedOption + '"');

  const now           = new Date().toISOString();
  const newReplyCount = ((contact.reply_count) || 0) + 1;

  let newStatus;
  if (votedOption.includes('rent'))                                                         newStatus = 'interested_rent';
  else if (votedOption.includes('sell'))                                                    newStatus = 'interested_sell';
  else if (votedOption.includes('market') || votedOption.includes('data') || votedOption.includes('report')) newStatus = 'wants_report';
  else                                                                                      newStatus = 'opted_out';

  await supabaseFetch('/owner_contacts?id=eq.' + contact.id, {
    method:  'PATCH',
    headers: { 'Prefer': 'return=minimal' },
    body:    JSON.stringify({ message_status: newStatus, reply_count: newReplyCount, replied_at: now }),
  });

  const agentRes = await supabaseFetch(
    '/profiles?id=eq.' + contact.assigned_agent + '&select=first_name,green_api_url,green_api_instance_id,green_api_token'
  );
  const agentRows    = await agentRes.json();
  const agentProfile = agentRows[0];
  if (!agentProfile || !agentProfile.green_api_instance_id) return;

  if (newStatus === 'opted_out') {
    await sendViaGreenApi(agentProfile, fromNumber,
      "No problem at all — you've been removed and won't hear from us again. Have a great day! 👋");
    console.log('[webhook/poll] ' + fromNumber + ' opted out via poll');
    return;
  }

  if (newStatus === 'wants_report') {
    // Don't auto-send a report — flag the contact and let the agent build/send
    // the report manually from the Intelligence Hub. The dashboard surfaces
    // 'wants_report' so the agent sees the request.
    await sendViaGreenApi(agentProfile, fromNumber,
      "I'll put together a detailed market report for your building and send it to you shortly. Watch this space!");
    console.log('[webhook/poll] ' + fromNumber + ' wants market report for "' + (contact.building_name || 'unknown') + '" — flagged for agent follow-up');
    return;
  }

  const settingsRes = await supabaseFetch('/api_settings?id=eq.1&select=gemini_api_key');
  const settingsRows = await settingsRes.json();
  const geminiKey   = settingsRows[0] && settingsRows[0].gemini_api_key ? settingsRows[0].gemini_api_key : '';

  const intent     = newStatus === 'interested_rent' ? 'rent' : 'sell';
  const votedLabel = intent === 'rent' ? 'rent it out' : 'sell it';

  const replyText = geminiKey
    ? await generateReply(contact.generated_message, 'I want to ' + votedLabel, agentProfile.first_name, geminiKey)
    : null;

  const fallback = intent === 'rent'
    ? "Thanks for letting me know! I'd love to help you get it rented. When would be a good time for a quick 5-minute call?"
    : "Great — the market is strong right now. Can we jump on a quick call this week? I can walk you through what your unit could realistically achieve.";

  await sendViaGreenApi(agentProfile, fromNumber, replyText || fallback);
  console.log('[webhook/poll] Sent ' + intent + ' follow-up to ' + fromNumber);
}

// ── Plain text reply handler ──────────────────────────────────────────────────

async function handleTextReply(fromNumber, messageText, contact) {
  const intent        = detectIntent(messageText);
  const now           = new Date().toISOString();
  const newReplyCount = ((contact.reply_count) || 0) + 1;

  console.log('[webhook/text] ' + fromNumber + ' replied — intent: ' + intent);

  let newStatus;
  if (intent === 'stop')        newStatus = 'opted_out';
  else if (intent === 'market') newStatus = 'wants_report';
  else if (intent === 'rent')   newStatus = 'interested_rent';
  else if (intent === 'sell')   newStatus = 'interested_sell';
  else                          newStatus = 'replied';

  await supabaseFetch('/owner_contacts?id=eq.' + contact.id, {
    method:  'PATCH',
    headers: { 'Prefer': 'return=minimal' },
    body:    JSON.stringify({ message_status: newStatus, reply_count: newReplyCount, replied_at: now }),
  });

  const agentRes = await supabaseFetch(
    '/profiles?id=eq.' + contact.assigned_agent + '&select=first_name,green_api_url,green_api_instance_id,green_api_token'
  );
  const agentRows    = await agentRes.json();
  const agentProfile = agentRows[0];
  if (!agentProfile || !agentProfile.green_api_instance_id) return;

  if (intent === 'stop') {
    await sendViaGreenApi(agentProfile, fromNumber,
      "No problem — you've been removed from our list and won't hear from us again. Have a great day!");
    return;
  }

  if (intent === 'market') {
    await sendViaGreenApi(agentProfile, fromNumber,
      "I'll put together a detailed market report for your building and send it to you shortly. Watch this space!");
    console.log('[webhook/text] ' + fromNumber + ' wants market report for "' + (contact.building_name || 'unknown') + '" — flagged for agent follow-up');
    return;
  }

  const settingsRes  = await supabaseFetch('/api_settings?id=eq.1&select=gemini_api_key');
  const settingsRows = await settingsRes.json();
  const geminiKey    = settingsRows[0] && settingsRows[0].gemini_api_key ? settingsRows[0].gemini_api_key : '';
  if (!geminiKey) return;

  const replyText = await generateReply(contact.generated_message, messageText, agentProfile.first_name, geminiKey);
  if (replyText) {
    await sendViaGreenApi(agentProfile, fromNumber, replyText);
    console.log('[webhook/text] Gemini reply sent to ' + fromNumber + ' (intent: ' + intent + ')');
  }
}

// ── stateInstanceChanged handler ──────────────────────────────────────────────
// Green API sends this when an instance's WhatsApp authorization changes.
// We update the agent's status in Supabase immediately so the dashboard
// reflects the true state rather than waiting for the next health check.

async function handleStateInstanceChanged(payload) {
  const instanceId = payload.instanceData && String(payload.instanceData.idInstance);
  const newState   = payload.stateInstance; // 'authorized', 'notAuthorized', 'blocked', etc.

  if (!instanceId || !newState) return;

  const newStatus = newState === 'authorized' ? 'connected' : 'disconnected';

  try {
    const res = await supabaseFetch(
      '/profiles?green_api_instance_id=eq.' + encodeURIComponent(instanceId) + '&select=id,first_name'
    );
    const rows = await res.json();
    if (!rows || !rows.length) {
      console.log('[webhook/state] No agent found for instance ' + instanceId);
      return;
    }
    const agent = rows[0];
    await supabaseFetch('/profiles?id=eq.' + agent.id, {
      method:  'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body:    JSON.stringify({ whatsapp_session_status: newStatus }),
    });
    console.log('[webhook/state] Agent ' + (agent.first_name || agent.id) +
      ' (instance ' + instanceId + ') → ' + newState + ' → status=' + newStatus);
  } catch (e) {
    console.error('[webhook/state] Error updating status for instance ' + instanceId + ':', e.message);
  }
}

// ── Main incoming webhook route ───────────────────────────────────────────────

router.post('/incoming', async function(req, res) {
  res.sendStatus(200); // Always respond immediately — Green API retries on non-200

  try {
    const payload = req.body;

    // ── Real-time instance state changes ─────────────────────────────────────
    if (payload.typeWebhook === 'stateInstanceChanged') {
      await handleStateInstanceChanged(payload);
      return;
    }

    if (payload.typeWebhook !== 'incomingMessageReceived') return;

    const rawSender  = (payload.senderData && (payload.senderData.sender || payload.senderData.chatId)) || '';
    const isGroupMsg = rawSender.endsWith('@g.us');

    // ── Elvi group collector — silently capture developer group messages ───────
    if (isGroupMsg) {
      const { ingestGroupMessage } = require('../services/groupCollector');
      const msgData = payload.messageData || {};
      const msg = {
        idMessage:   payload.idMessage,
        type:        msgData.typeMessage,
        textMessage: (msgData.textMessageData && msgData.textMessageData.textMessage) ||
                     (msgData.extendedTextMessageData && msgData.extendedTextMessageData.text) || '',
        caption:     (msgData.fileMessageData && msgData.fileMessageData.caption) ||
                     (msgData.imageMessageData && msgData.imageMessageData.caption) || '',
        fileName:    (msgData.fileMessageData && msgData.fileMessageData.fileName) || '',
        timestamp:   payload.timestamp,
      };
      ingestGroupMessage(rawSender, msg).catch(function(err) {
        console.warn('[webhook/group] Elvi ingest error:', err.message);
      });
      return;
    }

    const fromNumber = rawSender.replace('@c.us', '').replace(/\D/g, '');
    if (!fromNumber) return;

    const msgType    = (payload.messageData && payload.messageData.typeMessage) || '';
    const isPollVote = msgType === 'pollUpdateMessage';
    const messageText = (
      (payload.messageData && payload.messageData.textMessageData && payload.messageData.textMessageData.textMessage) ||
      (payload.messageData && payload.messageData.extendedTextMessageData && payload.messageData.extendedTextMessageData.text) ||
      ''
    ).trim();

    const lookupRes = await supabaseFetch(
      '/owner_contacts?number_1=eq.' + encodeURIComponent(fromNumber) +
      '&message_status=in.(sent,replied,interested_rent,interested_sell)' +
      '&order=sent_at.desc&limit=1' +
      '&select=id,reply_count,generated_message,assigned_agent,building_name'
    );
    const contacts = await lookupRes.json();
    const contact  = Array.isArray(contacts) && contacts.length > 0 ? contacts[0] : null;

    await supabaseFetch('/incoming_messages', {
      method: 'POST',
      body: JSON.stringify({
        contact_id:   contact ? contact.id : null,
        from_number:  fromNumber,
        message_text: isPollVote ? '[POLL VOTE] ' + msgType : messageText,
        raw_payload:  payload,
        matched:      !!contact,
      }),
    }).catch(function(e) { console.error('[webhook] Log error:', e.message); });

    if (!contact) return;

    if (isPollVote) {
      await handlePollVote(payload, fromNumber, contact);
    } else if (messageText) {
      await handleTextReply(fromNumber, messageText, contact);
    }

  } catch (err) {
    console.error('[webhook] Unhandled error:', err.message);
  }
});

module.exports = router;
