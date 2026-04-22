'use strict';

const express     = require('express');
const router      = express.Router();
const multer      = require('multer');
const { execSync }= require('child_process');
const fs          = require('fs');
const path        = require('path');
const os          = require('os');
const { createClient } = require('@supabase/supabase-js');

// ── Supabase client ──────────────────────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// ── Multer — memory storage ──────────────────────────────────────────────────
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 },
}).fields([
  { name: 'csv_file',        maxCount: 1 },
  { name: 'rental_csv_file', maxCount: 1 },
  { name: 'image_file',      maxCount: 1 },
]);

// ── Helpers ──────────────────────────────────────────────────────────────────
const SCRIPT = path.join(__dirname, '..', '..', 'scripts', 'run_report.py');

function writeTmpJson(obj, tag, ts) {
  const p = path.join(os.tmpdir(), `eva_${tag}_${ts}.json`);
  fs.writeFileSync(p, JSON.stringify(obj));
  return p;
}

function runPython(args, timeoutMs = 90_000) {
  const cmd = `python3 "${SCRIPT}" ${args.map(a => `"${a}"`).join(' ')}`;
  const raw  = execSync(cmd, { encoding: 'utf8', timeout: timeoutMs });
  return JSON.parse(raw.trim());
}

async function getGeminiKey() {
  try {
    const { data } = await supabase
      .from('api_settings')
      .select('gemini_api_key')
      .eq('id', 1)
      .single();
    return data?.gemini_api_key || process.env.GEMINI_API_KEY || '';
  } catch {
    return process.env.GEMINI_API_KEY || '';
  }
}

// ── POST /api/market-reports/generate ───────────────────────────────────────
router.post('/generate', (req, res) => {
  upload(req, res, async (uploadErr) => {
    if (uploadErr) return res.status(400).json({ error: uploadErr.message });

    const tempFiles = [];

    try {
      // ── 1. Parse & validate ───────────────────────────────────────────────
      const {
        report_type            = 'single',
        report_period          = '',
        agent_name             = 'EVA Real Estate',
        agent_contact          = 'info@evadxb.com',
        agent_id,
        custom_location_notes  = '',
        personalisation_prompt = '',
        agent_instruction      = '',
        service_charge_psf     = '',
      } = req.body;

      const community = req.body.community_name || req.body.community || '';

      if (!req.files?.csv_file?.[0]) {
        return res.status(400).json({ error: 'A Property Monitor CSV file is required.' });
      }
      if (!agent_id) {
        return res.status(400).json({ error: 'agent_id is required.' });
      }

      const communities = report_type === 'comparison'
        ? (Array.isArray(req.body['communities[]'])
            ? req.body['communities[]']
            : [req.body['communities[]']].filter(Boolean))
        : [community].filter(Boolean);

      if (communities.length === 0) {
        return res.status(400).json({ error: 'At least one community name is required.' });
      }

      const primaryCommunity = communities[0];
      const ts = Date.now();

      // ── 2. Save uploaded files to /tmp ────────────────────────────────────
      const csvPath = path.join(os.tmpdir(), `pm_${ts}.csv`);
      fs.writeFileSync(csvPath, req.files.csv_file[0].buffer);
      tempFiles.push(csvPath);

      let rentalCsvPath = null;
      if (req.files?.rental_csv_file?.[0]) {
        rentalCsvPath = path.join(os.tmpdir(), `pm_rental_${ts}.csv`);
        fs.writeFileSync(rentalCsvPath, req.files.rental_csv_file[0].buffer);
        tempFiles.push(rentalCsvPath);
      }

      let imagePath     = null;
      let imageBase64   = null;
      let imageMimeType = null;
      if (req.files?.image_file?.[0]) {
        const imgFile = req.files.image_file[0];
        imagePath     = path.join(os.tmpdir(), `report_img_${ts}.jpg`);
        fs.writeFileSync(imagePath, imgFile.buffer);
        tempFiles.push(imagePath);
        imageBase64   = imgFile.buffer.toString('base64');
        imageMimeType = imgFile.mimetype || 'image/jpeg';
      }

      // ── 3. Python: parse CSV + run data analysis ──────────────────────────
      const analyseArgsPath = writeTmpJson(
        { communities, report_type, rental_csv_path: rentalCsvPath },
        'analyse_args', ts
      );
      tempFiles.push(analyseArgsPath);

      let analysisResult;
      try {
        analysisResult = runPython(['analyse', csvPath, analyseArgsPath], 60_000);
      } catch (e) {
        console.error('[analyse] Python error:', e.message);
        return res.status(500).json({
          error: 'Failed to analyse CSV. Make sure it is a valid Property Monitor export.',
          detail: e.message,
        });
      }

      if (analysisResult.error) {
        return res.status(500).json({ error: analysisResult.error });
      }

      const primaryData = report_type === 'comparison'
        ? (analysisResult.areas_data?.[0] || {})
        : analysisResult;

      // ── 4. Net yield calculation ──────────────────────────────────────────
      let netYield          = null;
      let serviceChargeNote = '';
      if (service_charge_psf && parseFloat(service_charge_psf) > 0) {
        const scVal     = parseFloat(service_charge_psf);
        const grossPct  = parseFloat(String(primaryData.avg_yield  || '').replace('%', '')) || 0;
        const avgPsfVal = parseFloat(String(primaryData.avg_psf    || '').replace(/[^\d.]/g, '')) || 0;
        if (grossPct > 0 && avgPsfVal > 0) {
          netYield = `${Math.max(0, grossPct - (scVal / avgPsfVal) * 100).toFixed(1)}%`;
        }
        serviceChargeNote = `AED ${scVal.toLocaleString('en-US', { maximumFractionDigits: 0 })}/sqft/year`;
      }

      // ── 5. Gemini narrative ───────────────────────────────────────────────
      const geminiKey = await getGeminiKey();
      let geminiData  = {};

      if (geminiKey) {
        const metricsLines = [
          `Community:               ${communities.join(', ')}`,
          `Total transactions:      ${primaryData.total_transactions || 'N/A'}`,
          `Total sales volume:      ${primaryData.total_volume       || 'N/A'}`,
          `Average sale price:      ${primaryData.avg_price          || 'N/A'}`,
          `Average PSF:             ${primaryData.avg_psf            || 'N/A'}`,
          `YoY price growth:        ${primaryData.yoy_growth         || 'N/A'}`,
          `Gross rental yield:      ${primaryData.avg_yield          || 'N/A'}`,
          netYield ? `Net yield (after SC):    ${netYield} (service charge ${serviceChargeNote})` : null,
        ].filter(Boolean).join('\n');

        const agentBlock = [
          custom_location_notes  ? `Agent observation: "${custom_location_notes}"` : null,
          agent_instruction      ? `Agent instruction: "${agent_instruction}". Incorporate naturally.` : null,
          (imageBase64 && personalisation_prompt)
            ? `Image uploaded. Agent note: "${personalisation_prompt}". Return image_placement as "cover" or "location_analysis" and write a short image_caption.`
            : null,
        ].filter(Boolean).join('\n\n');

        const prompt = `You are a senior Dubai real estate analyst writing content for a professional PDF investor report by EVA Real Estate LLC.

KEY MARKET DATA (from DLD / Property Monitor records):
${metricsLines}
${agentBlock ? `\nAGENT INSTRUCTIONS:\n${agentBlock}` : ''}

Return ONLY valid JSON, no markdown fences:
{
  "exec_summary": "100-120 word executive summary in plain English. Be specific about numbers. Explain what they mean for an investor. No jargon.",
  "outlook_items": [
    {"title": "What does the price trend tell us?", "body": "150-200 words on price momentum and 6-12 month outlook."},
    {"title": "Will there be more supply?", "body": "150-200 words on supply dynamics and structural constraints."},
    {"title": "What does this mean for rental income?", "body": "150-200 words covering gross yield, net return, AED rent growth, 3-5 year hold view."},
    {"title": "The broader Dubai picture", "body": "150-200 words on Dubai macro tailwinds and risk factors."}
  ]${imageBase64 && personalisation_prompt ? ',\n  "image_placement": "cover",\n  "image_caption": "1-sentence caption."' : ''}
}`;

        try {
          const parts = [{ text: prompt }];
          if (imageBase64 && personalisation_prompt) {
            parts.push({ inlineData: { mimeType: imageMimeType, data: imageBase64 } });
          }
          const gemRes = await fetch(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${geminiKey}`,
            {
              method:  'POST',
              headers: { 'Content-Type': 'application/json' },
              body:    JSON.stringify({ contents: [{ parts }], generationConfig: { temperature: 0.8 } }),
            }
          );
          const gemJson = await gemRes.json();
          const rawText = (gemJson?.candidates?.[0]?.content?.parts?.[0]?.text || '')
            .trim()
            .replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/i, '').trim();
          if (rawText) geminiData = JSON.parse(rawText);
        } catch (e) {
          console.warn('[Gemini] Error — report will generate without AI narrative:', e.message);
        }
      }

      // ── 6. Build full data dict for PDF generator ─────────────────────────
      const reportData = {
        report_type,
        communities,
        community:      primaryCommunity,
        report_date:    new Date().toLocaleDateString('en-GB', { month: 'long', year: 'numeric' }),
        report_period:  report_period || new Date().toLocaleDateString('en-GB', { month: 'long', year: 'numeric' }),
        agent_name,
        agent_contact,
        custom_location_notes,

        ...analysisResult,

        exec_summary:  geminiData.exec_summary  || '',
        outlook_items: geminiData.outlook_items || [],

        ...(netYield          ? { net_yield: netYield }                    : {}),
        ...(serviceChargeNote ? { service_charge_note: serviceChargeNote } : {}),

        ...(imagePath && geminiData.image_placement === 'cover'
          ? { cover_image_path: imagePath } : {}),
        ...(imagePath && geminiData.image_placement === 'location_analysis'
          ? { location_image_path: imagePath } : {}),
        ...(geminiData.image_caption ? { image_caption: geminiData.image_caption } : {}),
      };

      // ── 7. Python: generate PDF ───────────────────────────────────────────
      const pdfPath      = path.join(os.tmpdir(), `eva_report_${ts}.pdf`);
      const generateArgs = writeTmpJson({ data: reportData, rental_csv_path: rentalCsvPath }, 'gen_args', ts);
      tempFiles.push(pdfPath, generateArgs);

      try {
        runPython(['generate', csvPath, pdfPath, generateArgs], 120_000);
      } catch (e) {
        console.error('[generate] Python error:', e.message);
        return res.status(500).json({ error: 'PDF generation failed: ' + e.message });
      }

      // ── 8. Upload PDF to Supabase Storage ─────────────────────────────────
      const safeName    = primaryCommunity
        .replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 60);
      const storagePath = `reports/${agent_id}/${ts}_${safeName}.pdf`;
      const pdfBuffer   = fs.readFileSync(pdfPath);

      const { error: uploadError } = await supabase.storage
        .from('market-reports')
        .upload(storagePath, pdfBuffer, {
          contentType: 'application/pdf',
          upsert: false,
        });

      if (uploadError) {
        console.error('[storage] Upload error:', uploadError);
        return res.status(500).json({ error: 'Storage upload failed: ' + uploadError.message });
      }

      const { data: { publicUrl } } = supabase.storage
        .from('market-reports')
        .getPublicUrl(storagePath);

      // ── 9. Insert record into market_reports ──────────────────────────────
      const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();

      const { data: record, error: dbError } = await supabase
        .from('market_reports')
        .insert({
          created_by:    agent_id,
          communities:   communities,
          community_name: communities.join(', '),
          report_type,
          agent_name,
          file_url:      publicUrl,
          report_url:    publicUrl,
          storage_path:  storagePath,
          expires_at:    expiresAt,
        })
        .select()
        .single();

      if (dbError) {
        console.error('[db] Insert error:', dbError.message);
      }

      // ── 10. Respond ───────────────────────────────────────────────────────
      return res.json({
        success:    true,
        report_url: publicUrl,
        report_id:  record?.id ?? null,
        expires_at: expiresAt,
      });

    } catch (err) {
      console.error('[marketReports] Unhandled error:', err);
      return res.status(500).json({ error: err.message || 'Internal server error' });

    } finally {
      for (const f of tempFiles) {
        try { if (f && fs.existsSync(f)) fs.unlinkSync(f); } catch (_) {}
      }
    }
  });
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