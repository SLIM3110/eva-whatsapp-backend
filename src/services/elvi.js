'use strict';

/**
 * Elvi — EVA Intelligence Hub AI Service
 * Core ingestion pipeline + query handler
 *
 * Requires (run in eva-whatsapp-backend/):
 *   npm install pdf-parse mammoth xlsx
 */

const crypto = require('crypto');

// ── Config ────────────────────────────────────────────────────────────────────
const SUPABASE_URL   = process.env.SUPABASE_URL;
const SUPABASE_KEY   = process.env.SUPABASE_SERVICE_KEY;
const GEMINI_KEY     = process.env.GEMINI_API_KEY;
const ANTHROPIC_KEY  = process.env.ANTHROPIC_API_KEY;

const CHUNK_SIZE     = 1500;   // chars per chunk (~375 tokens)
const CHUNK_OVERLAP  = 200;    // overlap between chunks to preserve context
const EMBED_MODEL    = 'text-embedding-004';
const CLAUDE_MODEL   = 'claude-sonnet-4-6';
const MAX_CONTEXT_CHUNKS = 8;  // chunks sent to Claude per query

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

// ── Text extraction ───────────────────────────────────────────────────────────
// Supports: PDF, DOCX, XLSX, plain text, WhatsApp messages (string)
async function extractText(buffer, mimetype, filename) {
  const ext = (filename || '').split('.').pop().toLowerCase();

  // PDF
  if (mimetype === 'application/pdf' || ext === 'pdf') {
    try {
      const pdfParse = require('pdf-parse');
      const data = await pdfParse(buffer);
      return data.text || '';
    } catch (e) {
      console.warn('[elvi/extract] pdf-parse error:', e.message);
      return '';
    }
  }

  // DOCX
  if (
    mimetype === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
    ext === 'docx'
  ) {
    try {
      const mammoth = require('mammoth');
      const result  = await mammoth.extractRawText({ buffer });
      return result.value || '';
    } catch (e) {
      console.warn('[elvi/extract] mammoth error:', e.message);
      return '';
    }
  }

  // XLSX / XLS
  if (
    mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
    mimetype === 'application/vnd.ms-excel' ||
    ext === 'xlsx' || ext === 'xls'
  ) {
    try {
      const XLSX     = require('xlsx');
      const workbook = XLSX.read(buffer, { type: 'buffer' });
      const lines    = [];
      for (const sheetName of workbook.SheetNames) {
        const sheet = workbook.Sheets[sheetName];
        const csv   = XLSX.utils.sheet_to_csv(sheet);
        if (csv.trim()) lines.push(`[Sheet: ${sheetName}]\n${csv}`);
      }
      return lines.join('\n\n');
    } catch (e) {
      console.warn('[elvi/extract] xlsx error:', e.message);
      return '';
    }
  }

  // Plain text / CSV / markdown
  if (
    mimetype?.startsWith('text/') ||
    ['txt','csv','md'].includes(ext)
  ) {
    return buffer.toString('utf8');
  }

  // Fallback — try UTF-8
  try { return buffer.toString('utf8'); }
  catch { return ''; }
}

// ── Text chunking ─────────────────────────────────────────────────────────────
// Splits on paragraph/sentence boundaries where possible
function chunkText(text, maxChars = CHUNK_SIZE, overlap = CHUNK_OVERLAP) {
  const clean  = text.replace(/\r\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
  if (!clean) return [];

  const chunks = [];
  let start    = 0;

  while (start < clean.length) {
    let end = Math.min(start + maxChars, clean.length);

    // Try to break on a paragraph boundary
    if (end < clean.length) {
      const paraBreak = clean.lastIndexOf('\n\n', end);
      if (paraBreak > start + maxChars * 0.5) {
        end = paraBreak + 2;
      } else {
        // Fall back to sentence boundary
        const sentBreak = clean.lastIndexOf('. ', end);
        if (sentBreak > start + maxChars * 0.5) end = sentBreak + 2;
      }
    }

    const chunk = clean.slice(start, end).trim();
    if (chunk.length > 50) chunks.push(chunk); // skip tiny fragments
    start = end - overlap;
    if (start >= clean.length) break;
  }

  return chunks;
}

// ── Content hash ──────────────────────────────────────────────────────────────
function hashText(text) {
  return crypto.createHash('sha256').update(text).digest('hex');
}

// ── Gemini embedding ──────────────────────────────────────────────────────────
async function generateEmbedding(text) {
  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${EMBED_MODEL}:embedContent?key=${GEMINI_KEY}`,
    {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({
        model:   `models/${EMBED_MODEL}`,
        content: { parts: [{ text }] },
      }),
    }
  );
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Gemini embedding error [${res.status}]: ${body}`);
  }
  const data = await res.json();
  return data?.embedding?.values || null;
}

// ── Vector search via match_docs Postgres function ────────────────────────────
async function searchDocs(queryEmbedding, { developerId, projectId, buildingId, limit = MAX_CONTEXT_CHUNKS } = {}) {
  const res = await supabaseFetch('/rpc/match_docs', {
    method:  'POST',
    body:    JSON.stringify({
      query_embedding:  queryEmbedding,
      match_count:      limit,
      filter_developer: developerId || null,
      filter_project:   projectId   || null,
      filter_building:  buildingId  || null,
      include_stale:    false,
    }),
  });
  return res.json();
}

// ── Ingest a document ─────────────────────────────────────────────────────────
/**
 * Full ingestion pipeline:
 * extract text → chunk → embed each chunk → store in developer_docs
 *
 * @param {object} params
 * @param {Buffer} params.buffer         - file buffer
 * @param {string} params.mimetype       - MIME type
 * @param {string} params.filename       - original filename
 * @param {string} params.docName        - display name
 * @param {string} params.docType        - brochure | price_list | payment_plan | etc.
 * @param {string} params.source         - manual_upload | whatsapp_group
 * @param {string} params.sourceGroupName
 * @param {string} params.developerId    - required
 * @param {string} params.projectId      - optional
 * @param {string} params.buildingId     - optional
 * @param {string} params.docDate        - ISO date string
 * @param {string} params.uploadedBy     - agent/admin UUID
 * @param {string} params.fileUrl        - Supabase storage URL
 * @param {string} params.jobId          - ingestion_jobs.id for progress tracking
 * @returns {{ chunksIngested: number, duplicatesSkipped: number }}
 */
async function ingestDocument(params) {
  const {
    buffer, mimetype, filename,
    docName, docType = 'other',
    source = 'manual_upload', sourceGroupName,
    developerId, projectId, buildingId,
    docDate, uploadedBy, fileUrl,
    jobId,
  } = params;

  // 1. Extract text
  const rawText = buffer instanceof Buffer
    ? await extractText(buffer, mimetype, filename)
    : buffer; // already a string (WhatsApp message)

  if (!rawText || rawText.trim().length < 20) {
    console.warn(`[elvi/ingest] Could not extract usable text from "${docName}"`);
    return { chunksIngested: 0, duplicatesSkipped: 0 };
  }

  // 2. Chunk the text
  const chunks = chunkText(rawText);
  if (!chunks.length) return { chunksIngested: 0, duplicatesSkipped: 0 };

  console.log(`[elvi/ingest] "${docName}" → ${chunks.length} chunks`);

  // 3. Generate a shared doc_group_id for all chunks of this document
  const docGroupId = crypto.randomUUID();

  // 4. Mark any existing current docs with the same name + developer as not current
  //    (handles re-uploading a newer version of the same document)
  if (source === 'manual_upload' && developerId) {
    await supabaseFetch(
      `/developer_docs?doc_name=eq.${encodeURIComponent(docName)}&developer_id=eq.${developerId}&is_current=eq.true`,
      { method: 'PATCH', headers: { 'Prefer': 'return=minimal' }, body: JSON.stringify({ is_current: false }) }
    ).catch(e => console.warn('[elvi/ingest] Stale mark warning:', e.message));
  }

  let chunksIngested    = 0;
  let duplicatesSkipped = 0;

  // 5. Process each chunk
  for (let i = 0; i < chunks.length; i++) {
    const chunkText = chunks[i];
    const hash      = hashText(chunkText);

    // Duplicate check — skip if this exact chunk is already in DB
    const dupRes  = await supabaseFetch(`/developer_docs?content_hash=eq.${hash}&select=id&limit=1`);
    const dupRows = await dupRes.json();
    if (dupRows.length > 0) {
      duplicatesSkipped++;
      continue;
    }

    // Generate embedding
    let embedding;
    try {
      embedding = await generateEmbedding(chunkText);
    } catch (e) {
      console.error(`[elvi/ingest] Embedding failed for chunk ${i}:`, e.message);
      continue;
    }

    // Store chunk
    await supabaseFetch('/developer_docs', {
      method:  'POST',
      headers: { 'Prefer': 'return=minimal' },
      body:    JSON.stringify({
        doc_group_id:      docGroupId,
        developer_id:      developerId   || null,
        project_id:        projectId     || null,
        building_id:       buildingId    || null,
        doc_name:          docName,
        doc_type:          docType,
        source,
        source_group_name: sourceGroupName || null,
        doc_date:          docDate        || null,
        is_current:        true,
        uploaded_by:       uploadedBy    || null,
        file_url:          fileUrl       || null,
        chunk_text:        chunkText,
        chunk_index:       i,
        content_hash:      hash,
        embedding:         JSON.stringify(embedding),
      }),
    });

    chunksIngested++;

    // Update job progress every 5 chunks
    if (jobId && i % 5 === 0) {
      await supabaseFetch(`/ingestion_jobs?id=eq.${jobId}`, {
        method:  'PATCH',
        headers: { 'Prefer': 'return=minimal' },
        body:    JSON.stringify({ processed_items: chunksIngested }),
      }).catch(() => {});
    }
  }

  // Final job update
  if (jobId) {
    await supabaseFetch(`/ingestion_jobs?id=eq.${jobId}`, {
      method:  'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body:    JSON.stringify({
        status:          'completed',
        processed_items: chunksIngested,
        completed_at:    new Date().toISOString(),
      }),
    }).catch(() => {});
  }

  console.log(`[elvi/ingest] Done: ${chunksIngested} ingested, ${duplicatesSkipped} duplicates skipped`);
  return { chunksIngested, duplicatesSkipped };
}

// ── Ingest a plain text message (WhatsApp group message) ─────────────────────
async function ingestTextMessage(params) {
  const { text, docName, sourceGroupName, developerId, projectId } = params;
  return ingestDocument({
    buffer:          text,
    mimetype:        'text/plain',
    filename:        'message.txt',
    docName:         docName || `WhatsApp: ${sourceGroupName || 'group'}`,
    docType:         'whatsapp_msg',
    source:          'whatsapp_group',
    sourceGroupName: sourceGroupName || null,
    developerId,
    projectId:       projectId || null,
    buildingId:      null,
    fileUrl:         null,
    uploadedBy:      null,
  });
}

// ── Claude system prompt ──────────────────────────────────────────────────────
function buildSystemPrompt() {
  return `You are Elvi, the AI assistant for EVA Real Estate in Dubai. You help agents get instant, accurate answers about properties, developers, payment plans, market data, and real estate regulations.

YOUR RULES:
1. Answer only real estate related questions. If asked about anything unrelated, politely redirect.
2. Every factual claim must be grounded in the documents provided below. Never invent prices, dates, or project details.
3. Always cite your source at the end of your answer — state the document name and type (e.g. "Source: DAMAC Hills 2 Brochure 2025").
4. If the retrieved documents don't contain the answer, say so clearly and suggest the agent checks directly with the developer.
5. Be concise and clear — agents are busy. Lead with the answer, then provide supporting detail.

YOUR EXPERTISE:
- Dubai off-plan and secondary market properties
- Developer payment plans, handover dates, Oqood registration
- DLD fee structures (4% transfer fee, trustee fees), NOC process, title deed timelines
- UAE mortgage rules — LTV ratios for residents vs non-residents, Central Bank caps, off-plan bank financing
- Golden visa property threshold (2M AED minimum)
- Community comparisons — JVC, JLT, Business Bay, Downtown, Marina, DAMAC Hills, Creek Harbour, Dubai South, Sobha Hartland, and all major Dubai areas
- ROI calculations, rental yield estimates, service charge benchmarks
- Vastu compliance — entrance direction, kitchen SE, master bedroom SW, staircase placement, water bodies
- Fund repatriation rules for Indian and Pakistani buyers
- Ejari, DEWA, Empower (district cooling) connection process
- Refurbishment costs per sqft (basic / mid / premium), Dubai Municipality permit requirements
- Developer reputations — Emaar, DAMAC, Sobha, Ellington, Nakheel, Meraas, Aldar, Omniyat

TONE: Speak like a knowledgeable senior agent — direct, confident, professional. Not a generic chatbot.`;
}

// ── Query Elvi ────────────────────────────────────────────────────────────────
/**
 * Main query handler — embed question → search docs → Claude → save + return
 *
 * @param {object} params
 * @param {string} params.agentId     - UUID of the agent asking
 * @param {string} params.sessionId   - UUID grouping this conversation thread
 * @param {string} params.message     - the agent's question
 * @param {Array}  params.history     - prior messages [{role, message}] for context
 * @param {string} params.developerId - optional: scope search to a developer
 * @param {string} params.projectId   - optional: scope search to a project
 * @param {string} params.buildingId  - optional: scope search to a building
 * @returns {{ reply: string, sources: object[], conversationId: string }}
 */
async function queryElvi({ agentId, sessionId, message, history = [], developerId, projectId, buildingId }) {
  // 1. Embed the question
  const queryEmbedding = await generateEmbedding(message);

  // 2. Search the knowledge base
  const chunks = await searchDocs(queryEmbedding, { developerId, projectId, buildingId });

  // 3. Build context from retrieved chunks
  const contextBlocks = (chunks || []).map((c, i) => {
    const location = [
      c.doc_name,
      c.doc_type,
      c.doc_date ? `(${c.doc_date})` : '',
      c.source_group_name ? `[via ${c.source_group_name}]` : '',
    ].filter(Boolean).join(' ');
    return `[Document ${i + 1}: ${location}]\n${c.chunk_text}`;
  }).join('\n\n---\n\n');

  const hasContext = contextBlocks.length > 0;

  // 4. Build conversation history for Claude (last 6 exchanges max)
  const recentHistory = history.slice(-12).map(h => ({
    role:    h.role === 'user' ? 'user' : 'assistant',
    content: h.message,
  }));

  // 5. Build Claude messages array
  const userContent = hasContext
    ? `RETRIEVED DOCUMENTS:\n\n${contextBlocks}\n\n---\n\nAGENT QUESTION: ${message}`
    : `AGENT QUESTION: ${message}\n\nNote: No relevant documents were found in the knowledge base for this question.`;

  const messages = [
    ...recentHistory,
    { role: 'user', content: userContent },
  ];

  // 6. Call Claude
  const claudeRes = await fetch('https://api.anthropic.com/v1/messages', {
    method:  'POST',
    headers: {
      'x-api-key':         ANTHROPIC_KEY,
      'anthropic-version': '2023-06-01',
      'Content-Type':      'application/json',
    },
    body: JSON.stringify({
      model:      CLAUDE_MODEL,
      max_tokens: 1024,
      system:     buildSystemPrompt(),
      messages,
    }),
  });

  if (!claudeRes.ok) {
    const body = await claudeRes.text();
    throw new Error(`Claude API error [${claudeRes.status}]: ${body}`);
  }

  const claudeData = await claudeRes.json();
  const reply      = claudeData?.content?.[0]?.text?.trim() || 'Sorry, I could not generate a response.';

  // 7. Save user message + assistant reply to elvi_conversations
  const sourceIds = (chunks || []).map(c => c.id).filter(Boolean);

  const saveUser = supabaseFetch('/elvi_conversations', {
    method:  'POST',
    headers: { 'Prefer': 'return=representation' },
    body:    JSON.stringify({ session_id: sessionId, agent_id: agentId, role: 'user', message, sources: [] }),
  });

  const saveAssistant = supabaseFetch('/elvi_conversations', {
    method:  'POST',
    headers: { 'Prefer': 'return=representation' },
    body:    JSON.stringify({ session_id: sessionId, agent_id: agentId, role: 'assistant', message: reply, sources: sourceIds }),
  });

  const [, assistantRes] = await Promise.all([saveUser, saveAssistant]);
  const [assistantRow]   = await assistantRes.json();

  // 8. Build source metadata for the frontend to display
  const sources = (chunks || []).map(c => ({
    id:          c.id,
    docName:     c.doc_name,
    docType:     c.doc_type,
    docDate:     c.doc_date,
    similarity:  Math.round(c.similarity * 100),
    source:      c.source,
    sourceGroup: c.source_group_name,
  }));

  return {
    reply,
    sources,
    conversationId: assistantRow?.id || null,
    sessionId,
  };
}

// ── Exports ───────────────────────────────────────────────────────────────────
module.exports = {
  ingestDocument,
  ingestTextMessage,
  queryElvi,
  generateEmbedding,
  extractText,
  chunkText,
};
