'use strict';

/**
 * Elvi — EVA Intelligence Hub AI
 * API Routes
 *
 * Endpoints:
 *   POST   /api/elvi/query                      — agent chat query
 *   GET    /api/elvi/conversations/:agentId      — chat history for an agent
 *   DELETE /api/elvi/conversations/:agentId      — clear chat history for an agent
 *   POST   /api/elvi/upload                      — upload + ingest a document (admin)
 *   GET    /api/elvi/docs                        — list all ingested document groups
 *   DELETE /api/elvi/docs/:docGroupId            — delete all chunks for a doc
 *   GET    /api/elvi/developers                  — list developers
 *   POST   /api/elvi/developers                  — create developer
 *   PATCH  /api/elvi/developers/:id              — update developer
 *   DELETE /api/elvi/developers/:id              — delete developer
 *   GET    /api/elvi/projects                    — list projects (optional ?developerId=)
 *   POST   /api/elvi/projects                    — create project
 *   PATCH  /api/elvi/projects/:id                — update project
 *   DELETE /api/elvi/projects/:id                — delete project
 *   GET    /api/elvi/buildings                   — list buildings (optional ?projectId=)
 *   POST   /api/elvi/buildings                   — create building
 *   PATCH  /api/elvi/buildings/:id               — update building
 *   DELETE /api/elvi/buildings/:id               — delete building
 *   GET    /api/elvi/group-sources               — list WhatsApp group sources
 *   POST   /api/elvi/group-sources               — register a group source
 *   PATCH  /api/elvi/group-sources/:id           — update group source
 *   DELETE /api/elvi/group-sources/:id           — remove group source
 *   POST   /api/elvi/group-sources/:id/ingest    — trigger history ingestion for a group
 */

const express = require('express');
const multer  = require('multer');
const router  = express.Router();

const {
  ingestDocument,
  queryElvi,
  queryElviStream,
} = require('../services/elvi');

// ── Multer — in-memory storage for uploaded docs ──────────────────────────────
const upload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 50 * 1024 * 1024 }, // 50 MB max
  fileFilter: (_req, file, cb) => {
    const allowed = [
      'application/pdf',
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/vnd.ms-excel',
      'text/plain',
      'text/csv',
      'text/markdown',
    ];
    const ext = (file.originalname || '').split('.').pop().toLowerCase();
    const allowedExts = ['pdf', 'docx', 'xlsx', 'xls', 'txt', 'csv', 'md'];
    if (allowed.includes(file.mimetype) || allowedExts.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error(`Unsupported file type: ${file.mimetype}`));
    }
  },
});

// ── Supabase helper ───────────────────────────────────────────────────────────
async function supabaseFetch(path, options = {}) {
  const baseUrl    = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;
  const res = await fetch(`${baseUrl}/rest/v1${path}`, {
    ...options,
    headers: {
      'apikey':        serviceKey,
      'Authorization': `Bearer ${serviceKey}`,
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

// ── Auth middleware — require x-api-key header ────────────────────────────────
// Accept either WHATSAPP_API_KEY (the canonical key the rest of the backend
// uses) or API_SECRET_KEY (legacy name). The frontend sends WHATSAPP_API_KEY,
// so without this fallback every Elvi request returned 401 in production.
function requireApiKey(req, res, next) {
  const key = req.headers['x-api-key'];
  const expected = process.env.WHATSAPP_API_KEY || process.env.API_SECRET_KEY;
  if (!expected) {
    console.error('[elvi/auth] No WHATSAPP_API_KEY or API_SECRET_KEY env var — every request will 401');
    return res.status(500).json({ error: 'Server misconfigured: API key env var missing' });
  }
  if (!key || key !== expected) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

router.use(requireApiKey);

// =============================================================================
// QUERY — POST /api/elvi/query
// Body: { agentId, sessionId?, message, history?, developerId?, projectId?, buildingId? }
// =============================================================================
router.post('/query', async (req, res) => {
  const {
    agentId,
    sessionId,
    message,
    history      = [],
    developerId  = null,
    projectId    = null,
    buildingId   = null,
    stream       = false,
  } = req.body;

  if (!agentId || !message) {
    return res.status(400).json({ error: 'agentId and message are required' });
  }

  // ── Streaming mode — SSE ────────────────────────────────────────────────────
  if (stream) {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no'); // disable nginx buffering
    res.flushHeaders();

    const writeChunk = (payload) => {
      try {
        res.write(`data: ${JSON.stringify(payload)}\n\n`);
      } catch (_) { /* client disconnected */ }
    };

    try {
      await queryElviStream(
        { agentId, sessionId, message, history, developerId, projectId, buildingId },
        writeChunk
      );
    } catch (err) {
      console.error('[elvi/query/stream]', err.message);
      writeChunk({ type: 'error', error: err.message });
    } finally {
      res.end();
    }
    return;
  }

  // ── Non-streaming mode — JSON (unchanged) ───────────────────────────────────
  try {
    const result = await queryElvi({
      agentId,
      sessionId,
      message,
      history,
      developerId,
      projectId,
      buildingId,
    });
    res.json(result);
    // result shape: { reply: string, sources: [...], sessionId: string }
  } catch (err) {
    console.error('[elvi/query]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// CONVERSATIONS — GET /api/elvi/conversations/:agentId
// =============================================================================
router.get('/conversations/:agentId', async (req, res) => {
  try {
    const { agentId } = req.params;
    const { sessionId, limit = 50 } = req.query;

    let path = `/elvi_conversations?agent_id=eq.${agentId}&order=created_at.asc&limit=${limit}`;
    if (sessionId) path += `&session_id=eq.${sessionId}`;

    const dbRes = await supabaseFetch(path);
    const rows  = await dbRes.json();
    res.json(rows);
  } catch (err) {
    console.error('[elvi/conversations GET]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// CONVERSATIONS — DELETE /api/elvi/conversations/:agentId
// =============================================================================
router.delete('/conversations/:agentId', async (req, res) => {
  try {
    const { agentId } = req.params;
    await supabaseFetch(`/elvi_conversations?agent_id=eq.${agentId}`, {
      method:  'DELETE',
      headers: { 'Prefer': 'return=minimal' },
    });
    res.json({ success: true });
  } catch (err) {
    console.error('[elvi/conversations DELETE]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// UPLOAD — POST /api/elvi/upload
// multipart/form-data fields:
//   file          — the document file
//   docName       — display name for the document
//   docType       — brochure | price_list | payment_plan | floor_plan | fact_sheet | market_report | legal | other
//   developerId   — required UUID
//   projectId     — optional UUID
//   buildingId    — optional UUID
//   docDate       — optional YYYY-MM-DD
//   uploadedBy    — agent/admin UUID
//   isVersionOf   — optional: doc_group_id of the document this replaces
// =============================================================================
router.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const {
      docName,
      docType     = 'other',
      developerId,
      projectId   = null,
      buildingId  = null,
      docDate     = null,
      uploadedBy  = null,
      isVersionOf = null,
    } = req.body;

    if (!docName) {
      return res.status(400).json({ error: 'docName is required' });
    }
    if (!developerId) {
      return res.status(400).json({ error: 'developerId is required' });
    }

    const result = await ingestDocument({
      buffer:      req.file.buffer,
      mimetype:    req.file.mimetype,
      filename:    req.file.originalname,
      docName,
      docType,
      developerId,
      projectId,
      buildingId,
      docDate,
      uploadedBy,
      isVersionOf,
      source:      'manual_upload',
    });

    res.json(result);
    // result shape: { docGroupId, chunksInserted, duplicatesSkipped }
  } catch (err) {
    console.error('[elvi/upload]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// DOCS LIST — GET /api/elvi/docs
// ?developerId= ?projectId= ?buildingId= ?docType=
// Returns one row per doc_group_id (deduplicated)
// =============================================================================
router.get('/docs', async (req, res) => {
  try {
    const { developerId, projectId, buildingId, docType } = req.query;

    let path = `/developer_docs?is_current=eq.true&select=doc_group_id,doc_name,doc_type,source,doc_date,developer_id,project_id,building_id,created_at&order=created_at.desc`;

    if (developerId) path += `&developer_id=eq.${developerId}`;
    if (projectId)   path += `&project_id=eq.${projectId}`;
    if (buildingId)  path += `&building_id=eq.${buildingId}`;
    if (docType)     path += `&doc_type=eq.${docType}`;

    const dbRes = await supabaseFetch(path);
    const rows  = await dbRes.json();

    // Deduplicate by doc_group_id (multiple chunks per doc — take first row per group)
    const seen = new Set();
    const docs = rows.filter(r => {
      if (seen.has(r.doc_group_id)) return false;
      seen.add(r.doc_group_id);
      return true;
    });

    res.json(docs);
  } catch (err) {
    console.error('[elvi/docs GET]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// DOC DELETE — DELETE /api/elvi/docs/:docGroupId
// Deletes all chunks for this doc_group_id
// =============================================================================
router.delete('/docs/:docGroupId', async (req, res) => {
  try {
    const { docGroupId } = req.params;
    await supabaseFetch(`/developer_docs?doc_group_id=eq.${docGroupId}`, {
      method:  'DELETE',
      headers: { 'Prefer': 'return=minimal' },
    });
    res.json({ success: true, docGroupId });
  } catch (err) {
    console.error('[elvi/docs DELETE]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// DEVELOPERS CRUD
// =============================================================================

// GET /api/elvi/developers
router.get('/developers', async (req, res) => {
  try {
    const dbRes = await supabaseFetch('/developers?order=name.asc');
    res.json(await dbRes.json());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/elvi/developers
// Body: { name, logoUrl?, website?, primaryContact?, notes? }
router.post('/developers', async (req, res) => {
  try {
    const { name, logoUrl, website, primaryContact, notes } = req.body;
    if (!name) return res.status(400).json({ error: 'name is required' });

    const dbRes = await supabaseFetch('/developers', {
      method:  'POST',
      headers: { 'Prefer': 'return=representation' },
      body:    JSON.stringify({
        name,
        logo_url:        logoUrl        || null,
        website:         website        || null,
        primary_contact: primaryContact || null,
        notes:           notes          || null,
      }),
    });
    const rows = await dbRes.json();
    res.status(201).json(rows[0] || rows);
  } catch (err) {
    console.error('[elvi/developers POST]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/elvi/developers/:id
router.patch('/developers/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { name, logoUrl, website, primaryContact, notes } = req.body;

    const body = {};
    if (name            !== undefined) body.name             = name;
    if (logoUrl         !== undefined) body.logo_url         = logoUrl;
    if (website         !== undefined) body.website          = website;
    if (primaryContact  !== undefined) body.primary_contact  = primaryContact;
    if (notes           !== undefined) body.notes            = notes;

    const dbRes = await supabaseFetch(`/developers?id=eq.${id}`, {
      method:  'PATCH',
      headers: { 'Prefer': 'return=representation' },
      body:    JSON.stringify(body),
    });
    const rows = await dbRes.json();
    res.json(rows[0] || rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/elvi/developers/:id
router.delete('/developers/:id', async (req, res) => {
  try {
    await supabaseFetch(`/developers?id=eq.${req.params.id}`, {
      method:  'DELETE',
      headers: { 'Prefer': 'return=minimal' },
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// PROJECTS CRUD
// =============================================================================

// GET /api/elvi/projects  (optional ?developerId=)
router.get('/projects', async (req, res) => {
  try {
    const { developerId } = req.query;
    let path = '/projects?order=name.asc';
    if (developerId) path += `&developer_id=eq.${developerId}`;
    const dbRes = await supabaseFetch(path);
    res.json(await dbRes.json());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/elvi/projects
router.post('/projects', async (req, res) => {
  try {
    const {
      developerId, name, community, location, type,
      status, handoverDate, totalUnits, notes,
    } = req.body;

    if (!developerId || !name) {
      return res.status(400).json({ error: 'developerId and name are required' });
    }

    const dbRes = await supabaseFetch('/projects', {
      method:  'POST',
      headers: { 'Prefer': 'return=representation' },
      body:    JSON.stringify({
        developer_id:  developerId,
        name,
        community:     community    || null,
        location:      location     || null,
        type:          type         || 'residential',
        status:        status       || 'off-plan',
        handover_date: handoverDate || null,
        total_units:   totalUnits   || null,
        notes:         notes        || null,
      }),
    });
    const rows = await dbRes.json();
    res.status(201).json(rows[0] || rows);
  } catch (err) {
    console.error('[elvi/projects POST]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/elvi/projects/:id
router.patch('/projects/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = ['name','community','location','type','status','handover_date','total_units','notes'];
    const body = {};
    // camelCase → snake_case map
    const fieldMap = {
      name: 'name', community: 'community', location: 'location',
      type: 'type', status: 'status',
      handoverDate: 'handover_date', totalUnits: 'total_units', notes: 'notes',
    };
    for (const [camel, snake] of Object.entries(fieldMap)) {
      if (req.body[camel] !== undefined) body[snake] = req.body[camel];
    }

    const dbRes = await supabaseFetch(`/projects?id=eq.${id}`, {
      method:  'PATCH',
      headers: { 'Prefer': 'return=representation' },
      body:    JSON.stringify(body),
    });
    const rows = await dbRes.json();
    res.json(rows[0] || rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/elvi/projects/:id
router.delete('/projects/:id', async (req, res) => {
  try {
    await supabaseFetch(`/projects?id=eq.${req.params.id}`, {
      method:  'DELETE',
      headers: { 'Prefer': 'return=minimal' },
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// BUILDINGS CRUD
// =============================================================================

// GET /api/elvi/buildings  (optional ?projectId= or ?developerId=)
router.get('/buildings', async (req, res) => {
  try {
    const { projectId, developerId } = req.query;
    let path = '/buildings?order=name.asc';
    if (projectId)   path += `&project_id=eq.${projectId}`;
    if (developerId) path += `&developer_id=eq.${developerId}`;
    const dbRes = await supabaseFetch(path);
    res.json(await dbRes.json());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/elvi/buildings
router.post('/buildings', async (req, res) => {
  try {
    const { projectId, developerId, name, floors, totalUnits, notes } = req.body;
    if (!projectId || !developerId || !name) {
      return res.status(400).json({ error: 'projectId, developerId, and name are required' });
    }

    const dbRes = await supabaseFetch('/buildings', {
      method:  'POST',
      headers: { 'Prefer': 'return=representation' },
      body:    JSON.stringify({
        project_id:   projectId,
        developer_id: developerId,
        name,
        floors:       floors      || null,
        total_units:  totalUnits  || null,
        notes:        notes       || null,
      }),
    });
    const rows = await dbRes.json();
    res.status(201).json(rows[0] || rows);
  } catch (err) {
    console.error('[elvi/buildings POST]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/elvi/buildings/:id
router.patch('/buildings/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const fieldMap = { name:'name', floors:'floors', totalUnits:'total_units', notes:'notes' };
    const body = {};
    for (const [camel, snake] of Object.entries(fieldMap)) {
      if (req.body[camel] !== undefined) body[snake] = req.body[camel];
    }
    const dbRes = await supabaseFetch(`/buildings?id=eq.${id}`, {
      method:  'PATCH',
      headers: { 'Prefer': 'return=representation' },
      body:    JSON.stringify(body),
    });
    const rows = await dbRes.json();
    res.json(rows[0] || rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/elvi/buildings/:id
router.delete('/buildings/:id', async (req, res) => {
  try {
    await supabaseFetch(`/buildings?id=eq.${req.params.id}`, {
      method:  'DELETE',
      headers: { 'Prefer': 'return=minimal' },
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// GROUP SOURCES — WhatsApp group management
// =============================================================================

// GET /api/elvi/group-sources
router.get('/group-sources', async (req, res) => {
  try {
    const dbRes = await supabaseFetch('/group_sources?order=date_added.desc');
    res.json(await dbRes.json());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/elvi/group-sources
// Body: { groupJid, groupName, developerId? }
router.post('/group-sources', async (req, res) => {
  try {
    const { groupJid, groupName, developerId = null } = req.body;
    if (!groupJid || !groupName) {
      return res.status(400).json({ error: 'groupJid and groupName are required' });
    }

    const dbRes = await supabaseFetch('/group_sources', {
      method:  'POST',
      headers: { 'Prefer': 'return=representation' },
      body:    JSON.stringify({
        group_jid:    groupJid,
        group_name:   groupName,
        developer_id: developerId,
        active:       true,
      }),
    });
    const rows = await dbRes.json();
    res.status(201).json(rows[0] || rows);
  } catch (err) {
    console.error('[elvi/group-sources POST]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/elvi/group-sources/:id
router.patch('/group-sources/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const fieldMap = {
      groupName:   'group_name',
      developerId: 'developer_id',
      active:      'active',
    };
    const body = {};
    for (const [camel, snake] of Object.entries(fieldMap)) {
      if (req.body[camel] !== undefined) body[snake] = req.body[camel];
    }
    const dbRes = await supabaseFetch(`/group_sources?id=eq.${id}`, {
      method:  'PATCH',
      headers: { 'Prefer': 'return=representation' },
      body:    JSON.stringify(body),
    });
    const rows = await dbRes.json();
    res.json(rows[0] || rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/elvi/group-sources/:id
router.delete('/group-sources/:id', async (req, res) => {
  try {
    await supabaseFetch(`/group_sources?id=eq.${req.params.id}`, {
      method:  'DELETE',
      headers: { 'Prefer': 'return=minimal' },
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// TRIGGER HISTORY INGESTION — POST /api/elvi/group-sources/:id/ingest
// Kicks off background history pull for a registered group
// =============================================================================
router.post('/group-sources/:id/ingest', async (req, res) => {
  try {
    const { id } = req.params;

    // Fetch the group source record
    const srcRes = await supabaseFetch(`/group_sources?id=eq.${id}`);
    const [source] = await srcRes.json();

    if (!source) return res.status(404).json({ error: 'Group source not found' });
    if (!source.active) return res.status(400).json({ error: 'Group source is inactive' });

    // Mark as ingestion started
    await supabaseFetch(`/group_sources?id=eq.${id}`, {
      method:  'PATCH',
      headers: { 'Prefer': 'return=minimal' },
      body:    JSON.stringify({ last_ingested_at: new Date().toISOString() }),
    });

    // Kick off in background — don't await so HTTP returns immediately
    const { ingestGroupHistory } = require('../services/groupCollector');
    ingestGroupHistory(source).catch(err =>
      console.error(`[elvi/ingest] Background history ingestion failed for ${source.group_jid}:`, err.message)
    );

    res.json({
      success: true,
      message: `History ingestion started for "${source.group_name}"`,
      groupJid: source.group_jid,
    });
  } catch (err) {
    console.error('[elvi/group-sources/ingest]', err.message);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
