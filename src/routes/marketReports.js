'use strict';

const express  = require('express');
const router   = express.Router();
const multer   = require('multer');
const { execSync } = require('child_process');
const fs       = require('fs');
const path     = require('path');
const os       = require('os');
const { createClient } = require('@supabase/supabase-js');
const { GoogleGenerativeAI } = require('@google/generative-ai');

// ── Clients ─────────────────────────────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// ── Multer — memory storage, accept CSV + optional image ───────────────────
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }, // 20 MB max
}).fields([
  { name: 'csv_file',         maxCount: 1 },
  { name: 'rental_csv_file',  maxCount: 1 },
  { name: 'image_file',       maxCount: 1 },
]);

// ── Helpers ──────────────────────────────────────────────────────────────────
const SCRIPT = path.join(__dirname, '..', '..', 'scripts', 'run_report.py');

/** Write a JSON args file to /tmp, return its path. */
function writeTmpJson(obj, tag, ts) {
  const p = path.join(os.tmpdir(), `eva_${tag}_${ts}.json`);
  fs.writeFileSync(p, JSON.stringify(obj));
  return p;
}

/** Run Python script and return parsed stdout JSON. Throws on non-zero exit. */
function runPython(args, timeoutMs = 90_000) {
  const cmd = ['python3', SCRIPT, ...args.map(a => `"${a}"`)].join(' ');
  const raw = execSync(cmd, { encoding: 'utf8', timeout: timeoutMs });
  return JSON.parse(raw.trim());
}

// ── POST /market-reports/generate ───────────────────────────────────────────
router.post('/generate', (req, res) => {
  upload(req, res, async (uploadErr) => {
    if (uploadErr) {
      return res.status(400).json({ error: uploadErr.message });
    }

    const tempFiles = [];

    try {
      // ── 1. Parse & validate form fields ──────────────────────────────────
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

      // Accept both legacy 'community' and new 'community_name' field names
      const community = req.body.community || req.body.community_name || '';

      if (!req.files?.csv_file?.[0]) {
        return res.status(400).json({ error: 'A Property Monitor CSV file is required.' });
      }
      if (!agent_id) {
        return res.status(400).json({ error: 'agent_id is required.' });
      }

      // Comparison mode sends communities[] as repeated form entries
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

      // ── 2. Save CSVs to /tmp ──────────────────────────────────────────────
      const csvPath = path.join(os.tmpdir(), `pm_${ts}.csv`);
      fs.writeFileSync(csvPath, req.files.csv_file[0].buffer);
      tempFiles.push(csvPath);

      let rentalCsvPath = null;
      if (req.files?.rental_csv_file?.[0]) {
        rentalCsvPath = path.join(os.tmpdir(), `pm_rental_${ts}.csv`);
        fs.writeFileSync(rentalCsvPath, req.files.rental_csv_file[0].buffer);
        tempFiles.push(rentalCsvPath);
      }

      // ── 3. Python: parse CSV + run all data analysis ──────────────────────
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
        });
      }

      if (analysisResult.error) {
        return res.status(500).json({ error: analysisResult.error });
      }

      // Primary area data (used for Gemini prompt)
      const primaryData = report_type === 'comparison'
        ? (analysisResult.areas_data?.[0] || {})
        : analysisResult;

      // ── 4. Compute service-charge-adjusted net yield ──────────────────────
      let netYield         = null;
      let serviceChargeNote = '';

      if (service_charge_psf && parseFloat(service_charge_psf) > 0) {
        const scVal      = parseFloat(service_charge_psf);
        const grossStr   = String(primaryData.avg_yield  || '').replace('%', '');
        const avgPsfStr  = String(primaryData.avg_psf    || '').replace(/[^\d.]/g, '');
        const grossPct   = parseFloat(grossStr)  || 0;
        const avgPsfVal  = parseFloat(avgPsfStr) || 0;

        if (grossPct > 0 && avgPsfVal > 0) {
          const scPct = (scVal / avgPsfVal) * 100;
          netYield    = `${Math.max(0, grossPct - scPct).toFixed(1)}%`;
        }
        serviceChargeNote = `AED ${scVal.toLocaleString('en-US', { maximumFractionDigits: 0 })}/sqft/year`;
      }

      // ── 5. Handle optional image ──────────────────────────────────────────
      let imageBase64   = null;
      let imageMimeType = null;
      let imagePath     = null;

      if (req.files?.image_file?.[0]) {
        const imgFile = req.files.image_file[0];
        imagePath     = path.join(os.tmpdir(), `report_img_${ts}.jpg`);
        fs.writeFileSync(imagePath, imgFile.buffer);
        tempFiles.push(imagePath);
        imageBase64   = imgFile.buffer.toString('base64');
        imageMimeType = imgFile.mimetype || 'image/jpeg';
      }

      // ── 6. Build Gemini prompt ────────────────────────────────────────────
      const metricsLines = [
        `Community:               ${communities.join(', ')}`,
        `Report period:           ${report_period || 'latest data'}`,
        `Total transactions:      ${primaryData.total_transactions || 'N/A'}`,
        `Total sales volume:      ${primaryData.total_volume       || 'N/A'}`,
        `Average sale price:      ${primaryData.avg_price          || 'N/A'}`,
        `Average PSF:             ${primaryData.avg_psf            || 'N/A'}`,
        `YoY price growth:        ${primaryData.yoy_growth         || 'N/A'}`,
        `Gross rental yield:      ${primaryData.avg_yield          || 'N/A'}`,
        netYield
          ? `Net yield (after SC):    ${netYield} (service charge ${serviceChargeNote})`
          : null,
        primaryData.view_premium_data?.length
          ? `Top view premiums: ${primaryData.view_premium_data
              .slice(0, 3)
              .map(([v, p]) => `${v} @ AED ${p.toLocaleString()}`)
              .join(', ')}`
          : null,
        primaryData.phase_data?.length
          ? `Phase leaders by PSF: ${primaryData.phase_data
              .slice(0, 2)
              .map(r => `${r[0]}: ${r[3]}`)
              .join(', ')}`
          : null,
      ].filter(Boolean).join('\n');

      const agentBlocks = [
        custom_location_notes
          ? `Agent observation to weave into the report: "${custom_location_notes}"`
          : null,
        agent_instruction
          ? `The agent has requested the following specific information be included: "${agent_instruction}". Incorporate this naturally.`
          : null,
        (imageBase64 && personalisation_prompt)
          ? `An image has been uploaded. Agent instruction for the image: "${personalisation_prompt}". Return "image_placement" as "cover" or "location_analysis" and write a short "image_caption".`
          : null,
      ].filter(Boolean).join('\n\n');

      const geminiPrompt = `You are a senior Dubai real estate analyst writing content for a professional PDF investor report distributed by EVA Real Estate LLC.

KEY MARKET DATA (calculated from DLD / Property Monitor transaction records):
${metricsLines}
${agentBlocks ? `\nAGENT INSTRUCTIONS:\n${agentBlocks}` : ''}

TASK — return ONLY valid JSON, no markdown fences, no commentary:
{
  "exec_summary": "100–120 word executive summary in plain English. Be specific about the numbers. Explain what they mean for an investor. No jargon. Write as flowing prose, not bullets.",
  "outlook_items": [
    {
      "title": "What does the price trend tell us?",
      "body": "150–200 words. Explain price momentum from the data. What does the PSF trend signal? What can a buyer or investor realistically expect over the next 6–12 months?"
    },
    {
      "title": "Will there be more supply?",
      "body": "150–200 words. Explain supply dynamics. If this is a villa or townhouse community, make the structural constraint argument clearly — you cannot build the same product in the same location twice."
    },
    {
      "title": "What does this mean for rental income?",
      "body": "150–200 words. Cover gross yield, the impact of service charges on net return, absolute AED rent growth even if percentage yield compresses, and what a 3–5 year hold period looks like."
    },
    {
      "title": "The broader Dubai picture",
      "body": "150–200 words. Dubai macro tailwinds: visa reform, HNW population inflows, zero income tax, infrastructure spend, Golden Visa programme. Keep grounded — acknowledge global risk factors briefly."
    }
  ]${imageBase64 && personalisation_prompt ? ',\n  "image_placement": "cover",\n  "image_caption": "Write a 1-sentence caption for the image."' : ''}
}`;

      // ── 7. Call Gemini ────────────────────────────────────────────────────
      let geminiData = {};
      try {
        const model  = genAI.getGenerativeModel({ model: 'gemini-1.5-flash' });
        const parts  = [{ text: geminiPrompt }];
        if (imageBase64 && personalisation_prompt) {
          parts.push({ inlineData: { mimeType: imageMimeType, data: imageBase64 } });
        }
        const result    = await model.generateContent(parts);
        const rawText   = result.response.text().trim()
          .replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/i, '').trim();
        geminiData = JSON.parse(rawText);
      } catch (e) {
        console.error('[Gemini] Error — report will generate without AI narrative:', e.message);
      }

      // ── 8. Build full data dict for PDF generator ─────────────────────────
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
          ? { cover_image_path: imagePath }
          : {}),
        ...(imagePath && geminiData.image_placement === 'location_analysis'
          ? { location_image_path: imagePath }
          : {}),
        ...(geminiData.image_caption ? { image_caption: geminiData.image_caption } : {}),
      };

      // ── 9. Python: generate PDF ───────────────────────────────────────────
      const pdfPath      = path.join(os.tmpdir(), `eva_report_${ts}.pdf`);
      const generateArgs = writeTmpJson({ data: reportData, rental_csv_path: rentalCsvPath }, 'gen_args', ts);
      tempFiles.push(pdfPath, generateArgs);

      try {
        runPython(['generate', csvPath, pdfPath, generateArgs], 120_000);
      } catch (e) {
        console.error('[generate] Python error:', e.message);
        return res.status(500).json({ error: 'PDF generation failed: ' + e.message });
      }

      // ── 10. Upload PDF to Supabase Storage ────────────────────────────────
      const safeName = primaryCommunity
        .replace(/\s+/g, '_')
        .replace(/[^a-zA-Z0-9_-]/g, '')
        .slice(0, 60);
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

      // ── 11. Insert record into market_reports table ───────────────────────
      const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();

      const { data: record, error: dbError } = await supabase
        .from('market_reports')
        .insert({
          agent_id,
          community_name: communities.join(', '),
          report_type,
          agent_name,
          report_url:    publicUrl,
          storage_path:  storagePath,
          expires_at:    expiresAt,
        })
        .select()
        .single();

      if (dbError) {
        console.error('[db] Insert error:', dbError.message);
      }

      // ── 12. Respond ───────────────────────────────────────────────────────
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

module.exports = router;
