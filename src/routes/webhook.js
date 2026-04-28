'use strict';

const express = require('express');
const router  = express.Router();

const FETCH_TIMEOUT_MS = 10000;

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
// Updates contact status only. The dashboard surfaces the lead — the agent
// replies manually. No automatic outbound from the webhook.

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
}

// ── Plain text reply handler ──────────────────────────────────────────────────
// Updates contact status only. The dashboard surfaces the lead — the agent
// replies manually. No automatic outbound from the webhook.

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
