'use strict';

const express = require('express');
const router  = express.Router();
const multer  = require('multer');
const fs      = require('fs');
const path    = require('path');
const os      = require('os');
const { createClient } = require('@supabase/supabase-js');
const { marketReportQueue } = require('../queue/marketReportQueue');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

const MAX_AREAS = 6;
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 },
}).fields([
  { name: 'csv_file',        maxCount: 1 },
  { name: 'rental_csv_file', maxCount: 1 },
  { name: 'image_file',      maxCount: 1 },
  ...Array.from({ length: MAX_AREAS }, (_, i) => ({
    name: 'area_csv_' + (i + 1),        maxCount: 1,
  })),
  ...Array.from({ length: MAX_AREAS }, (_, i) => ({
    name: 'area_rental_csv_' + (i + 1), maxCount: 1,
  })),
]);

// ── POST /api/market-reports/generate ───────────────────────────────────────
// Parses uploads, writes them to /tmp, enqueues a background job, and returns
// the jobId immediately. The worker process consumes the job and runs the full
// analyse → Gemini → generate → upload → insert pipeline.
router.post('/generate', (req, res) => {
  upload(req, res, async (uploadErr) => {
    if (uploadErr) return res.status(400).json({ error: uploadErr.message });

    const tempFiles = [];

    function cleanupOnError() {
      for (const f of tempFiles) {
        try { if (f && fs.existsSync(f)) fs.unlinkSync(f); } catch (_) {}
      }
    }

    try {
      const {
        report_type            = 'single',
        report_period          = '',
        agent_name             = 'EVA Real Estate',
        agent_contact          = 'info@evadxb.com',
        agent_id,
        client_name            = '',
        audience               = 'neutral',
        custom_location_notes  = '',
        personalisation_prompt = '',
        agent_instruction      = '',
        service_charge_psf     = '',
      } = req.body;

      const community = req.body.community_name || req.body.community || '';

      if (!agent_id) {
        return res.status(400).json({ error: 'agent_id is required.' });
      }

      // The frontend exposes a single "Community Notes & AI Instructions"
      // field so the agent can add facts about this specific community
      // (e.g. "Mudon Al Ranim is G+1 only — no high-floor language"). Map
      // it through to custom_location_notes (used as a verbatim callout in
      // the Location section) when not separately provided.
      const merged_location_notes = custom_location_notes || agent_instruction || '';

      const ts = Date.now();

      // ── Per-area CSV upload (new comparison-mode flow) ─────────────────────
      const areaCsvs = [];
      for (let i = 1; i <= MAX_AREAS; i++) {
        const file = req.files && req.files['area_csv_' + i] && req.files['area_csv_' + i][0];
        if (!file) continue;
        const name = (req.body['area_name_' + i] || '').trim();
        if (!name) {
          cleanupOnError();
          return res.status(400).json({
            error: 'area_csv_' + i + ' was uploaded but area_name_' + i + ' is missing — every per-area CSV needs a community/area name.',
          });
        }
        const p = path.join(os.tmpdir(), 'pm_area' + i + '_' + ts + '.csv');
        fs.writeFileSync(p, file.buffer);
        tempFiles.push(p);

        let areaRentalPath = null;
        const areaRentalFile = req.files && req.files['area_rental_csv_' + i] && req.files['area_rental_csv_' + i][0];
        if (areaRentalFile) {
          areaRentalPath = path.join(os.tmpdir(), 'pm_area' + i + '_rental_' + ts + '.csv');
          fs.writeFileSync(areaRentalPath, areaRentalFile.buffer);
          tempFiles.push(areaRentalPath);
        }

        areaCsvs.push({ csv_path: p, rental_csv_path: areaRentalPath, community_name: name });
      }

      const usingPerAreaCsvs = areaCsvs.length > 0;

      if (!usingPerAreaCsvs && !(req.files && req.files.csv_file && req.files.csv_file[0])) {
        cleanupOnError();
        return res.status(400).json({
          error: 'A Property Monitor CSV file is required (csv_file for single/legacy mode, or area_csv_1..area_csv_6 for the new comparison flow).',
        });
      }

      let communities;
      if (usingPerAreaCsvs) {
        communities = areaCsvs.map(a => a.community_name);
      } else if (report_type === 'comparison') {
        communities = (
          Array.isArray(req.body['communities[]'])
            ? req.body['communities[]']
            : [req.body['communities[]']].filter(Boolean)
        );
      } else {
        communities = [community].filter(Boolean);
      }

      if (communities.length === 0) {
        cleanupOnError();
        return res.status(400).json({ error: 'At least one community/area name is required.' });
      }

      const primaryCommunity = communities[0];

      // ── Save legacy uploaded files to /tmp ────────────────────────────────
      let csvPath;
      if (usingPerAreaCsvs) {
        csvPath = areaCsvs[0].csv_path;
      } else {
        csvPath = path.join(os.tmpdir(), 'pm_' + ts + '.csv');
        fs.writeFileSync(csvPath, req.files.csv_file[0].buffer);
        tempFiles.push(csvPath);
      }

      let rentalCsvPath = null;
      if (usingPerAreaCsvs) {
        rentalCsvPath = areaCsvs[0].rental_csv_path;
      } else if (req.files && req.files.rental_csv_file && req.files.rental_csv_file[0]) {
        rentalCsvPath = path.join(os.tmpdir(), 'pm_rental_' + ts + '.csv');
        fs.writeFileSync(rentalCsvPath, req.files.rental_csv_file[0].buffer);
        tempFiles.push(rentalCsvPath);
      }

      let imagePath     = null;
      let imageMimeType = null;
      if (req.files && req.files.image_file && req.files.image_file[0]) {
        const imgFile = req.files.image_file[0];
        imagePath     = path.join(os.tmpdir(), 'report_img_' + ts + '.jpg');
        fs.writeFileSync(imagePath, imgFile.buffer);
        tempFiles.push(imagePath);
        imageMimeType = imgFile.mimetype || 'image/jpeg';
      }

      // ── Build job payload (paths + form fields only, no buffers) ──────────
      const jobData = {
        ts: ts,
        report_type: report_type,
        report_period: report_period,
        agent_name: agent_name,
        agent_contact: agent_contact,
        agent_id: String(agent_id),
        client_name: client_name,
        audience: audience,
        custom_location_notes: merged_location_notes,
        personalisation_prompt: personalisation_prompt,
        agent_instruction: agent_instruction,
        service_charge_psf: service_charge_psf,
        communities: communities,
        primaryCommunity: primaryCommunity,
        csvPath: csvPath,
        rentalCsvPath: rentalCsvPath,
        imagePath: imagePath,
        imageMimeType: imageMimeType,
        areaCsvs: areaCsvs,
        usingPerAreaCsvs: usingPerAreaCsvs,
        tempFiles: tempFiles,
      };

      const job = await marketReportQueue.add('generate', jobData, {
        removeOnComplete: { age: 900 },
        removeOnFail:     { age: 3600 },
        attempts: 1,
      });

      return res.json({ success: true, jobId: job.id });

    } catch (err) {
      console.error('[marketReports] Enqueue error:', err);
      cleanupOnError();
      return res.status(500).json({ error: err.message || 'Internal server error' });
    }
  });
});

// ── GET /api/market-reports/status/:jobId ───────────────────────────────────
// Caller must pass ?agent_id=... matching the job's agent_id, otherwise 404
// (404 not 403, to prevent jobId enumeration confirming existence).
router.get('/status/:jobId', async (req, res) => {
  try {
    const agentId = req.query.agent_id;
    if (!agentId) return res.status(400).json({ error: 'agent_id required' });

    const job = await marketReportQueue.getJob(req.params.jobId);
    if (!job || String(job.data.agent_id) !== String(agentId)) {
      return res.status(404).json({ error: 'job not found' });
    }

    const state = await job.getState();

    let position = null;
    if (state === 'waiting') {
      const waiting = await marketReportQueue.getWaiting();
      const idx = waiting.findIndex(j => j.id === job.id);
      position = idx === -1 ? null : idx;
    }

    const response = { status: state, position: position };
    if (state === 'completed' && job.returnvalue) {
      response.report_url = job.returnvalue.report_url;
      response.report_id  = job.returnvalue.report_id;
      response.expires_at = job.returnvalue.expires_at;
    }
    if (state === 'failed') {
      response.error = job.failedReason || 'Report generation failed';
    }

    return res.json(response);
  } catch (err) {
    console.error('[marketReports] Status error:', err);
    return res.status(500).json({ error: err.message || 'Status lookup failed' });
  }
});

// ── GET /api/market-reports — list reports for the calling agent ─────────────
router.get('/', async (req, res) => {
  const agentId = req.query.agent_id;
  if (!agentId) return res.status(400).json({ error: 'agent_id required' });

  const { data, error } = await supabase
    .from('market_reports')
    .select('id, community_name, report_type, agent_name, report_url, created_at, expires_at')
    .eq('created_by', agentId)
    .order('created_at', { ascending: false })
    .limit(50);

  if (error) return res.status(500).json({ error: error.message });
  return res.json({ reports: data });
});

module.exports = router;
