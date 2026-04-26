'use strict';

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '..', '.env') });

const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const { createClient } = require('@supabase/supabase-js');

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const PROJECT_ROOT = path.join(__dirname, '..', '..');
const SCRIPT = path.join(PROJECT_ROOT, 'scripts', 'run_report.py');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

const connection = new IORedis(REDIS_URL, {
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
});

function runPython(args, timeoutMs) {
  return new Promise(function (resolve, reject) {
    const child = spawn('python3', [SCRIPT].concat(args), {
      cwd: PROJECT_ROOT,
      env: process.env,
    });

    let stdout = '';
    let stderr = '';
    child.stdout.on('data', function (d) { stdout += d.toString('utf8'); });
    child.stderr.on('data', function (d) { stderr += d.toString('utf8'); });

    const timer = setTimeout(function () {
      try { child.kill('SIGKILL'); } catch (_) {}
      reject(new Error('Python ' + args[0] + ' timed out after ' + timeoutMs + 'ms'));
    }, timeoutMs);

    child.on('close', function (code) {
      clearTimeout(timer);
      if (code !== 0) {
        const tail = (stderr || stdout || '').slice(-2000);
        return reject(new Error('Python ' + args[0] + ' exited ' + code + ': ' + tail));
      }
      try {
        resolve(JSON.parse(stdout.trim()));
      } catch (e) {
        reject(new Error('Failed to parse Python output: ' + stdout.slice(0, 1000)));
      }
    });

    child.on('error', function (err) {
      clearTimeout(timer);
      reject(err);
    });
  });
}

async function getGeminiKey() {
  try {
    const { data } = await supabase
      .from('api_settings')
      .select('gemini_api_key')
      .eq('id', 1)
      .single();
    return (data && data.gemini_api_key) || process.env.GEMINI_API_KEY || '';
  } catch (_) {
    return process.env.GEMINI_API_KEY || '';
  }
}

async function processJob(job) {
  const d = job.data;
  const tempFiles = Array.isArray(d.tempFiles) ? d.tempFiles.slice() : [];

  try {
    // 1. Analyse
    const analyseArgsPath = path.join(os.tmpdir(), 'eva_analyse_args_' + d.ts + '.json');
    fs.writeFileSync(analyseArgsPath, JSON.stringify({
      communities: d.communities,
      report_type: d.report_type,
      rental_csv_path: d.rentalCsvPath,
      area_csvs: d.usingPerAreaCsvs ? d.areaCsvs : null,
    }));
    tempFiles.push(analyseArgsPath);

    const analysisResult = await runPython(['analyse', d.csvPath, analyseArgsPath], 60000);
    if (analysisResult && analysisResult.error) {
      throw new Error(analysisResult.error);
    }

    const primaryData = d.report_type === 'comparison'
      ? ((analysisResult.areas_data && analysisResult.areas_data[0]) || {})
      : analysisResult;

    // 2. Net yield
    let netYield = null;
    let serviceChargeNote = '';
    if (d.service_charge_psf && parseFloat(d.service_charge_psf) > 0) {
      const scVal = parseFloat(d.service_charge_psf);
      const grossPct = parseFloat(String(primaryData.avg_yield || '').replace('%', '')) || 0;
      const avgPsfVal = parseFloat(String(primaryData.avg_psf || '').replace(/[^\d.]/g, '')) || 0;
      if (grossPct > 0 && avgPsfVal > 0) {
        netYield = Math.max(0, grossPct - (scVal / avgPsfVal) * 100).toFixed(1) + '%';
      }
      serviceChargeNote = 'AED ' + scVal.toLocaleString('en-US', { maximumFractionDigits: 0 }) + '/sqft/year';
    }

    // 3. Image base64 (only loaded into memory inside the worker, not via Redis)
    let imageBase64 = null;
    const imageMimeType = d.imageMimeType || 'image/jpeg';
    if (d.imagePath && fs.existsSync(d.imagePath)) {
      try {
        imageBase64 = fs.readFileSync(d.imagePath).toString('base64');
      } catch (e) {
        console.warn('[worker] failed to read image: ' + e.message);
      }
    }

    // 4. Gemini narrative
    const geminiKey = await getGeminiKey();
    let geminiData = {};

    if (geminiKey) {
      const metricsLines = [
        'Community:               ' + d.communities.join(', '),
        'Total transactions:      ' + (primaryData.total_transactions || 'N/A'),
        'Total sales volume:      ' + (primaryData.total_volume || 'N/A'),
        'Average sale price:      ' + (primaryData.avg_price || 'N/A'),
        'Average PSF:             ' + (primaryData.avg_psf || 'N/A'),
        'YoY price growth:        ' + (primaryData.yoy_growth || 'N/A'),
        'Gross rental yield:      ' + (primaryData.avg_yield || 'N/A'),
        netYield ? 'Net yield (after SC):    ' + netYield + ' (service charge ' + serviceChargeNote + ')' : null,
      ].filter(Boolean).join('\n');

      const agentBlock = [
        d.custom_location_notes ? 'Agent observation: "' + d.custom_location_notes + '"' : null,
        d.agent_instruction ? 'Agent instruction: "' + d.agent_instruction + '". Incorporate naturally.' : null,
        (imageBase64 && d.personalisation_prompt)
          ? 'Image uploaded. Agent note: "' + d.personalisation_prompt + '". Return image_placement as "cover" or "location_analysis" and write a short image_caption.'
          : null,
      ].filter(Boolean).join('\n\n');

      const promptText =
        'You are a senior Dubai real estate analyst writing content for a professional PDF investor report by EVA Real Estate LLC.\n\n' +
        'KEY MARKET DATA (from DLD / Property Monitor records):\n' + metricsLines + '\n' +
        (agentBlock ? '\nAGENT INSTRUCTIONS:\n' + agentBlock + '\n' : '') +
        '\nReturn ONLY valid JSON, no markdown fences:\n' +
        '{\n' +
        '  "exec_summary": "100-120 word executive summary in plain English. Be specific about numbers. Explain what they mean for an investor. No jargon.",\n' +
        '  "outlook_items": [\n' +
        '    {"title": "What does the price trend tell us?", "body": "150-200 words on price momentum and 6-12 month outlook."},\n' +
        '    {"title": "Will there be more supply?", "body": "150-200 words on supply dynamics and structural constraints."},\n' +
        '    {"title": "What does this mean for rental income?", "body": "150-200 words covering gross yield, net return, AED rent growth, 3-5 year hold view."},\n' +
        '    {"title": "The broader Dubai picture", "body": "150-200 words on Dubai macro tailwinds and risk factors."}\n' +
        '  ]' + (imageBase64 && d.personalisation_prompt ? ',\n  "image_placement": "cover",\n  "image_caption": "1-sentence caption."' : '') + '\n' +
        '}';

      try {
        const parts = [{ text: promptText }];
        if (imageBase64 && d.personalisation_prompt) {
          parts.push({ inlineData: { mimeType: imageMimeType, data: imageBase64 } });
        }
        const gemRes = await fetch(
          'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=' + geminiKey,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ contents: [{ parts: parts }], generationConfig: { temperature: 0.8 } }),
          }
        );
        const gemJson = await gemRes.json();
        const rawText = ((gemJson && gemJson.candidates && gemJson.candidates[0] && gemJson.candidates[0].content && gemJson.candidates[0].content.parts && gemJson.candidates[0].content.parts[0] && gemJson.candidates[0].content.parts[0].text) || '')
          .trim()
          .replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/i, '').trim();
        if (rawText) geminiData = JSON.parse(rawText);
      } catch (e) {
        console.warn('[Gemini] Error — report will generate without AI narrative: ' + e.message);
      }
    }

    // 5. Build reportData
    const reportData = Object.assign({
      report_type: d.report_type,
      communities: d.communities,
      community: d.primaryCommunity,
      report_date: new Date().toLocaleDateString('en-GB', { month: 'long', year: 'numeric' }),
      report_period: d.report_period || new Date().toLocaleDateString('en-GB', { month: 'long', year: 'numeric' }),
      agent_name: d.agent_name,
      agent_contact: d.agent_contact,
      custom_location_notes: d.custom_location_notes,
    }, analysisResult, {
      exec_summary: geminiData.exec_summary || '',
      outlook_items: geminiData.outlook_items || [],
    });

    if (netYield) reportData.net_yield = netYield;
    if (serviceChargeNote) reportData.service_charge_note = serviceChargeNote;
    if (d.imagePath && geminiData.image_placement === 'cover') reportData.cover_image_path = d.imagePath;
    if (d.imagePath && geminiData.image_placement === 'location_analysis') reportData.location_image_path = d.imagePath;
    if (geminiData.image_caption) reportData.image_caption = geminiData.image_caption;

    // 6. Generate PDF
    const pdfPath = path.join(os.tmpdir(), 'eva_report_' + d.ts + '.pdf');
    const generateArgs = path.join(os.tmpdir(), 'eva_gen_args_' + d.ts + '.json');
    fs.writeFileSync(generateArgs, JSON.stringify({
      data: reportData,
      rental_csv_path: d.rentalCsvPath,
      area_csvs: d.usingPerAreaCsvs ? d.areaCsvs : null,
    }));
    tempFiles.push(pdfPath, generateArgs);

    await runPython(['generate', d.csvPath, pdfPath, generateArgs], 120000);

    // 7. Upload to Supabase Storage
    const safeName = d.primaryCommunity
      .replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 60);
    const storagePath = 'reports/' + d.agent_id + '/' + d.ts + '_' + safeName + '.pdf';
    const pdfBuffer = fs.readFileSync(pdfPath);

    const uploadRes = await supabase.storage
      .from('market-reports')
      .upload(storagePath, pdfBuffer, {
        contentType: 'application/pdf',
        upsert: false,
      });
    if (uploadRes.error) {
      throw new Error('Storage upload failed: ' + uploadRes.error.message);
    }

    const pubRes = supabase.storage.from('market-reports').getPublicUrl(storagePath);
    const publicUrl = pubRes.data.publicUrl;

    // 8. DB insert
    const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();
    const insertRes = await supabase
      .from('market_reports')
      .insert({
        created_by: d.agent_id,
        communities: d.communities,
        community_name: d.communities.join(', '),
        report_type: d.report_type,
        agent_name: d.agent_name,
        file_url: publicUrl,
        report_url: publicUrl,
        storage_path: storagePath,
        expires_at: expiresAt,
      })
      .select()
      .single();

    if (insertRes.error) {
      console.error('[db] Insert error: ' + insertRes.error.message);
    }

    return {
      report_url: publicUrl,
      report_id: (insertRes.data && insertRes.data.id) || null,
      expires_at: expiresAt,
    };
  } finally {
    for (const f of tempFiles) {
      try { if (f && fs.existsSync(f)) fs.unlinkSync(f); } catch (_) {}
    }
  }
}

const worker = new Worker('market-reports', processJob, {
  connection: connection,
  concurrency: 2,
});

worker.on('completed', function (job) {
  console.log('[worker] Job ' + job.id + ' completed');
});

worker.on('failed', function (job, err) {
  console.error('[worker] Job ' + (job && job.id ? job.id : '?') + ' failed: ' + (err && err.message ? err.message : err));
});

worker.on('error', function (err) {
  console.error('[worker] Worker error: ' + (err && err.message ? err.message : err));
});

let shuttingDown = false;
async function shutdown(sig) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('[worker] received ' + sig + ', closing gracefully (will finish in-flight jobs)...');
  try {
    await worker.close();
  } catch (e) {
    console.error('[worker] close error: ' + e.message);
  }
  process.exit(0);
}
process.on('SIGTERM', function () { shutdown('SIGTERM'); });
process.on('SIGINT', function () { shutdown('SIGINT'); });

console.log('[worker] eva-market-worker started — concurrency 2, redis ' + REDIS_URL);
