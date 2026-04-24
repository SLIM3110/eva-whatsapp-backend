'use strict';

/**
 * Elvi — WhatsApp Group Collector
 *
 * Silently monitors registered developer WhatsApp groups:
 *  1. ingestGroupHistory(source)   — pulls full historical messages on first registration
 *  2. ingestGroupMessage(message)  — called from webhook for live @g.us messages
 *
 * Each message passes a relevance filter (Claude) before being stored.
 * Documents shared in groups are downloaded and run through the full ingestion pipeline.
 */

const { ingestDocument, ingestTextMessage } = require('./elvi');

const ANTHROPIC_KEY  = process.env.ANTHROPIC_API_KEY;
const SUPABASE_URL   = process.env.SUPABASE_URL;
const SUPABASE_KEY   = process.env.SUPABASE_SERVICE_KEY;

// How far back to ingest history (days)
const HISTORY_DAYS   = 180;
// Batch size for history ingestion to avoid rate limits
const HISTORY_BATCH  = 50;

// ── Supabase helper ───────────────────────────────────────────────────────────
async function supabaseFetch(path, options = {}) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1${path}`, {
    ...options,
    headers: {
      'apikey':        SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Content-Type':  'application/json',
      ...(options.headers || {}),
    },
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Supabase ${options.method || 'GET'} ${path} [${res.status}]: ${body}`);
  }
  return res;
}

// ── Fetch group source by JID ─────────────────────────────────────────────────
async function getGroupSource(groupJid) {
  try {
    const res = await supabaseFetch(
      `/group_sources?group_jid=eq.${encodeURIComponent(groupJid)}&active=eq.true&limit=1`
    );
    const rows = await res.json();
    return Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
  } catch (e) {
    console.warn('[groupCollector] getGroupSource error:', e.message);
    return null;
  }
}

// ── Fetch agent credentials for a group's assigned developer ──────────────────
// We need a Green API instance to pull chat history — use any active agent instance
async function getActiveAgentCreds() {
  try {
    const res = await supabaseFetch(
      `/profiles?green_api_instance_id=not.is.null&select=green_api_url,green_api_instance_id,green_api_token&limit=1`
    );
    const rows = await res.json();
    return Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
  } catch (e) {
    console.warn('[groupCollector] getActiveAgentCreds error:', e.message);
    return null;
  }
}

// ── Relevance filter — ask Claude if a message is worth ingesting ─────────────
// Returns true if the message contains real estate intelligence worth keeping.
// This filters out greetings, chit-chat, and unrelated messages.
async function isRelevant(text) {
  if (!text || text.trim().length < 30) return false;
  if (!ANTHROPIC_KEY) return true; // No key — ingest everything

  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method:  'POST',
      headers: {
        'x-api-key':         ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Type':      'application/json',
      },
      body: JSON.stringify({
        model:      'claude-haiku-4-5-20251001',   // cheap fast model for classification
        max_tokens: 10,
        messages:   [{
          role:    'user',
          content: `You are a classifier for a Dubai real estate AI. Does the following WhatsApp message from a developer group contain useful real estate intelligence (project updates, pricing, payment plans, launches, handover dates, unit availability, promotions, documents, or market news)?

Message: "${text.slice(0, 600)}"

Reply with exactly one word: YES or NO.`,
        }],
      }),
    });

    if (!res.ok) return true; // On error, default to ingest
    const data  = await res.json();
    const reply = (data?.content?.[0]?.text || '').trim().toUpperCase();
    return reply === 'YES';
  } catch (e) {
    console.warn('[groupCollector] relevance filter error:', e.message);
    return true; // Default to ingest on error
  }
}

// ── Download file from Green API (document messages) ─────────────────────────
async function downloadFileFromGroup(agentCreds, chatId, messageId) {
  const { green_api_url, green_api_instance_id, green_api_token } = agentCreds;
  try {
    const res = await fetch(
      `${green_api_url}/waInstance${green_api_instance_id}/downloadFile/${green_api_token}`,
      {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ chatId, idMessage: messageId }),
      }
    );
    if (!res.ok) return null;
    const data = await res.json();
    // Returns { downloadUrl, fileName, mimeType }
    if (!data?.downloadUrl) return null;

    // Fetch the actual file bytes
    const fileRes = await fetch(data.downloadUrl);
    if (!fileRes.ok) return null;
    const buffer = Buffer.from(await fileRes.arrayBuffer());
    return {
      buffer,
      filename: data.fileName || 'document',
      mimetype: data.mimeType || 'application/octet-stream',
    };
  } catch (e) {
    console.warn('[groupCollector] downloadFileFromGroup error:', e.message);
    return null;
  }
}

// ── Process a single group message ───────────────────────────────────────────
async function processGroupMessage(msg, source, agentCreds) {
  const {
    developer_id: developerId,
    group_jid:    groupJid,
    group_name:   groupName,
    id:           groupSourceId,
  } = source;

  const msgType = msg.type || msg.typeMessage || 'textMessage';
  const chatId  = msg.chatId || `${groupJid}`;

  // ── Document / file message ──
  if (
    msgType === 'documentMessage' ||
    msgType === 'imageMessage' ||
    msgType === 'videoMessage'
  ) {
    const fileName = msg.fileName || msg.caption || `group_doc_${Date.now()}`;
    const caption  = msg.caption || msg.textMessage || '';
    const isDoc    = msgType === 'documentMessage';

    // Only ingest actual documents (PDFs, spreadsheets, Word docs)
    if (isDoc) {
      console.log(`[groupCollector] Document received in "${groupName}": ${fileName}`);

      const fileData = await downloadFileFromGroup(
        agentCreds, chatId, msg.idMessage
      );

      if (fileData) {
        try {
          await ingestDocument({
            buffer:      fileData.buffer,
            mimetype:    fileData.mimetype,
            filename:    fileData.filename,
            docName:     fileName,
            docType:     'other',    // Admin can reclassify later
            developerId: developerId || null,
            projectId:   null,
            buildingId:  null,
            docDate:     null,
            source:      'whatsapp_group',
            sourceGroupName: groupName,
          });
          console.log(`[groupCollector] Ingested doc: ${fileName} from "${groupName}"`);
        } catch (e) {
          console.error(`[groupCollector] ingestDocument error: ${e.message}`);
        }
      }
    }

    // Still ingest caption as text message if meaningful
    if (caption && caption.length > 30) {
      const relevant = await isRelevant(caption);
      if (relevant) {
        await ingestTextMessage({
          text:            caption,
          developerId:     developerId || null,
          source:          'whatsapp_group',
          sourceGroupName: groupName,
          docDate:         null,
        }).catch(e => console.warn('[groupCollector] ingestTextMessage error:', e.message));
      }
    }
    return;
  }

  // ── Text message ──
  const text = msg.textMessage || msg.text || '';
  if (!text) return;

  const relevant = await isRelevant(text);
  if (!relevant) {
    console.log(`[groupCollector] Skipping irrelevant message from "${groupName}"`);
    return;
  }

  try {
    await ingestTextMessage({
      text,
      developerId:     developerId || null,
      source:          'whatsapp_group',
      sourceGroupName: groupName,
      docDate:         null,
    });
    console.log(`[groupCollector] Ingested text message from "${groupName}" (${text.length} chars)`);
  } catch (e) {
    console.error(`[groupCollector] ingestTextMessage error: ${e.message}`);
  }
}

// ── Pull full chat history for a group ───────────────────────────────────────
// Called when a group is first registered or admin triggers a re-ingestion.
async function ingestGroupHistory(source) {
  const { group_jid: groupJid, group_name: groupName } = source;

  console.log(`[groupCollector] Starting history ingestion for "${groupName}" (${groupJid})`);

  const agentCreds = await getActiveAgentCreds();
  if (!agentCreds) {
    console.error('[groupCollector] No active Green API instance found — cannot pull history');
    return;
  }

  const { green_api_url, green_api_instance_id, green_api_token } = agentCreds;
  const chatId   = groupJid; // Groups use full JID with @g.us
  const minTime  = Math.floor((Date.now() - HISTORY_DAYS * 24 * 60 * 60 * 1000) / 1000);

  let count = 0;
  let lastMessageId = null;
  let hasMore = true;

  while (hasMore) {
    try {
      const body = {
        chatId,
        count: HISTORY_BATCH,
      };
      if (lastMessageId) body.minTime = minTime;

      const res = await fetch(
        `${green_api_url}/waInstance${green_api_instance_id}/getChatHistory/${green_api_token}`,
        {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify(body),
        }
      );

      if (!res.ok) {
        const txt = await res.text();
        console.error(`[groupCollector] getChatHistory error: ${txt.slice(0, 200)}`);
        break;
      }

      const messages = await res.json();
      if (!Array.isArray(messages) || messages.length === 0) {
        hasMore = false;
        break;
      }

      // Filter to messages within our time window
      const filtered = messages.filter(m => {
        const ts = m.timestamp || 0;
        return ts >= minTime;
      });

      if (filtered.length === 0) {
        hasMore = false;
        break;
      }

      // Process in sequence to avoid rate limits
      for (const msg of filtered) {
        await processGroupMessage(msg, source, agentCreds);
        count++;
        // Small delay to be gentle on APIs
        await new Promise(r => setTimeout(r, 200));
      }

      // Green API getChatHistory returns oldest-first when count is specified
      // Stop if we got fewer than the batch size
      if (messages.length < HISTORY_BATCH) {
        hasMore = false;
      } else {
        lastMessageId = messages[messages.length - 1].idMessage;
      }

    } catch (e) {
      console.error(`[groupCollector] History ingestion error: ${e.message}`);
      hasMore = false;
    }
  }

  // Mark history as ingested
  try {
    await supabaseFetch(`/group_sources?group_jid=eq.${encodeURIComponent(groupJid)}`, {
      method:  'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body:    JSON.stringify({
        history_ingested:  true,
        last_ingested_at:  new Date().toISOString(),
      }),
    });
  } catch (e) {
    console.warn('[groupCollector] Failed to update ingestion flag:', e.message);
  }

  console.log(`[groupCollector] History ingestion complete for "${groupName}" — ${count} messages processed`);
}

// ── Live message handler — called from webhook.js for @g.us messages ─────────
// webhook.js should call this when rawSender ends with @g.us
async function ingestGroupMessage(groupJid, msg) {
  const source = await getGroupSource(groupJid);
  if (!source) {
    // Group not registered — ignore silently
    return;
  }

  const agentCreds = await getActiveAgentCreds();
  if (!agentCreds) return;

  await processGroupMessage(msg, source, agentCreds);
}

module.exports = {
  ingestGroupHistory,
  ingestGroupMessage,
};
