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
    child.stderr.on('data', function (d) {
      const s = d.toString('utf8');
      stderr += s;
      // Also stream to PM2 logs in real time so diagnostic lines (e.g. the
      // [filter] community=... matched=N/M output) are visible during a run,
      // not just on failure.
      process.stderr.write(s);
    });

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
      const typology = (primaryData.property_typology || 'unknown').toString().toLowerCase();
      const metricsLines = [
        'Community:               ' + d.communities.join(', '),
        'Property typology:       ' + typology + ' (auto-detected from unit_type / floor_level / plot data)',
        'Total transactions:      ' + (primaryData.total_transactions || 'N/A'),
        'Total sales volume:      ' + (primaryData.total_volume || 'N/A'),
        'Average sale price:      ' + (primaryData.avg_price || 'N/A'),
        'Average PSF:             ' + (primaryData.avg_psf || 'N/A'),
        'YoY price growth:        ' + (primaryData.yoy_growth || 'N/A'),
        'Gross rental yield:      ' + (primaryData.avg_yield || 'N/A'),
        netYield ? 'Net yield (after SC):    ' + netYield + ' (service charge ' + serviceChargeNote + ')' : null,
      ].filter(Boolean).join('\n');

      let typologyConstraint = '';
      if (typology === 'villa' || typology === 'low-rise') {
        typologyConstraint =
          "TYPOLOGY CONSTRAINT — STRICT: this is a villa / townhouse / low-rise community. " +
          "Do NOT use language that only applies to apartment towers. Specifically, do NOT mention " +
          "'high floor', 'upper floors', 'penthouse', 'sea view from balcony', 'tower amenities', " +
          "or 'sky views'. Instead use position-within-cluster language where the data supports it: " +
          "single-row, end-unit, corner plot, park-facing, road-facing, garden-facing, back-to-back, " +
          "plot orientation, proximity to community amenities. If the data does not support a specific " +
          "claim about position, do not invent one.\n\n";
      } else if (typology === 'apartment') {
        typologyConstraint =
          "TYPOLOGY CONSTRAINT: this is an apartment community. Floor level, view orientation, " +
          "corner vs interior unit, and tower position are the relevant value drivers. " +
          "Do NOT mention 'plot size', 'single-row', 'road-facing villa', or 'end-of-row' framing.\n\n";
      } else if (typology === 'mixed') {
        typologyConstraint =
          "TYPOLOGY CONSTRAINT: mixed community (villas + apartments). Where position-based language " +
          "applies it must be tagged to villa stock; where floor-based language applies it must be " +
          "tagged to apartment stock. Do not blend the two.\n\n";
      }

      const agentBlock = [
        d.custom_location_notes ? 'Agent observation: "' + d.custom_location_notes + '"' : null,
        d.agent_instruction ? 'Agent instruction: "' + d.agent_instruction + '". Incorporate naturally.' : null,
        (imageBase64 && d.personalisation_prompt)
          ? 'Image uploaded. Agent note: "' + d.personalisation_prompt + '". Return image_placement as "cover" or "location_analysis" and write a short image_caption.'
          : null,
      ].filter(Boolean).join('\n\n');

      const community = d.primaryCommunity || (d.communities && d.communities[0]) || 'this Dubai community';

      const audience = (d.audience || 'neutral').toString().toLowerCase();
      let audienceFraming = '';
      if (audience === 'seller') {
        audienceFraming =
          "AUDIENCE: this briefing is for a SELLER who currently owns property in " + community + ". " +
          "Frame every observation through 'what is this market doing FOR a seller right now?'. " +
          "Where the data legitimately supports it, surface signals that favour selling — supply shortages, " +
          "accelerating demand, peak pricing windows, the opportunity cost of holding through a possible " +
          "rate or supply shift. Do not misrepresent data; if signals favour holding, say so plainly. " +
          "But where the evidence supports acting, use language that helps the owner decide to list now: " +
          "reference comparable transactions at attractive levels, days-on-market trends, the cost of waiting. " +
          "End every major paragraph with the practical takeaway for them AS A SELLER.\n\n";
      } else if (audience === 'buyer') {
        audienceFraming =
          "AUDIENCE: this briefing is for a BUYER evaluating acquisition in " + community + ". " +
          "Frame every observation through 'what is this market doing FOR a buyer right now?'. " +
          "Where the data legitimately supports it, surface entry-thesis signals — appreciation runway, " +
          "infrastructure tailwinds, supply pipeline tightness, demographic shifts strengthening rent rolls. " +
          "Do not cheerlead; if pricing looks stretched or yields are compressed, name it. " +
          "Where the evidence supports buying, build conviction with comparable PSF levels, named drivers, " +
          "and the macro backdrop. End every major paragraph with the practical takeaway AS A BUYER.\n\n";
      } else {
        audienceFraming =
          "AUDIENCE: neutral — this briefing should serve either a seller or buyer. " +
          "Be balanced; surface both sides where the data is mixed.\n\n";
      }

      const promptText = [
        audienceFraming + typologyConstraint +
        'You are a senior Dubai real estate analyst writing a personalised market briefing for a property owner in ' + community + '.',
        'Your audience is NOT an analyst. They own a property and want to understand WHAT is happening in their specific market and WHY, in plain language with concrete, named context — not generic statements.',
        '',
        'Use Google Search to ground every claim about the macro environment, infrastructure, regulation, or news in current, dated information. Reference specific developments where genuinely relevant to ' + community + ':',
        '- Dubai 2040 Urban Master Plan zoning shifts and the named district this community sits in',
        '- Population growth and Golden Visa programme expansions affecting buyer pools',
        '- Tax-free status (no capital gains, no income tax) and AED-USD peg implications for foreign capital',
        '- Transit and infrastructure under construction or planned: Metro Blue Line alignment and stations, Etihad Rail, road and bridge upgrades, named mobility projects within ~5km of this community',
        '- Major nearby developments: Dubai Islands, Palm Jebel Ali, Expo City, Dubai South, and named master-developer launches relevant to this area',
        '- Mortgage rate environment, CBUAE policy changes, recent stamp-duty or fee adjustments',
        '- Buyer-demographic shifts (UAE residents, GCC, European, South Asian, Russian, Chinese flows) and how they specifically affect this community',
        '- School catchments, retail and amenity build-out, employment hubs within commuting range',
        '',
        'KEY MARKET DATA for ' + community + ' (from DLD / Property Monitor):',
        metricsLines,
        agentBlock ? '\nAGENT INSTRUCTIONS:\n' + agentBlock : '',
        '',
        'Return ONLY valid JSON (no markdown fences, no commentary outside the JSON). Be specific, quantitative where possible, and tie every paragraph back to ' + community + ' rather than Dubai in general. Where you reference an infrastructure project, named development, or policy change, use its name.',
        '{',
        '  "exec_summary": "130-170 words. The big-picture briefing for this owner. Why is the market doing what it is doing right now in ' + community + '? Tie to at least two macro factors (visa/population, supply, infrastructure, capital flows). End with one forward-looking sentence covering 12-24 months.",',
        '  "metrics_narrative": "70-110 words. Read the four headline numbers above (transactions, avg price, avg PSF, gross yield) together. Compare to Dubai-wide medians where useful. What story do they tell about ' + community + ' right now?",',
        '  "volume_narrative": "70-110 words. Concrete reading of the monthly transaction volume trend. Is buyer activity rising / easing / stable, and what specifically is driving it? Reference a named factor (visa policy, schooling, transit project completion, developer launch nearby).",',
        '  "price_narrative": "70-110 words. What is moving the price line in ' + community + '? Reference at least one concrete driver (supply constraint, transit project, school/amenity build-out, capital flow, policy change). Avoid generic appreciation language.",',
        '  "market_outlook_narrative": "180-260 words. Read the four outlook indicators (Price Direction / Demand Level / Supply / Rental Outlook) for this owner. Reference at least two specific Dubai factors that affect ' + community + ' specifically — name them (e.g. specific Blue Line station, specific master-plan zoning change, specific developer pipeline, specific demographic flow). End with a clear practical takeaway: what should this owner do over the next 12 months — hold, list, refinance, or refurbish — and why.",',
        '  "outlook_items": [',
        '    {"title": "What does the price trend tell us?", "body": "150-200 words. ' + community + "'s position in the broader Dubai cycle. Be specific and quantitative.\"},",
        '    {"title": "Will there be more supply?", "body": "150-200 words. The concrete supply pipeline situation for ' + community + ' — remaining plots, master-plan zoning, recent developer launches, completion timelines, named projects."},',
        '    {"title": "What does this mean for rental income?", "body": "150-200 words. Tenant demand drivers tied to ' + community + ': school catchments, transit access, employer hubs, expat demographics."},',
        '    {"title": "The broader Dubai picture", "body": "150-200 words. Macro tailwinds and risks: Golden Visa, population, GDP, mortgage rates, AED peg, geopolitical capital flows. How specifically do these reach ' + community + '?"}',
        '  ]' + (imageBase64 && d.personalisation_prompt ? ',\n  "image_placement": "cover",\n  "image_caption": "1-sentence caption."' : ''),
        '}'
      ].join('\n');

      try {
        const parts = [{ text: promptText }];
        if (imageBase64 && d.personalisation_prompt) {
          parts.push({ inlineData: { mimeType: imageMimeType, data: imageBase64 } });
        }
        // Gemini 2.5 Flash with Google Search grounding so the analyst can
        // reference current Dubai infrastructure and policy news in the
        // narrative — not just whatever was in its training cutoff.
        const gemRes = await fetch(
          'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=' + geminiKey,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contents: [{ parts: parts }],
              tools: [{ googleSearch: {} }],
              generationConfig: { temperature: 0.7, maxOutputTokens: 8192 },
            }),
          }
        );
        const gemJson = await gemRes.json();
        const rawText = ((gemJson && gemJson.candidates && gemJson.candidates[0] && gemJson.candidates[0].content && gemJson.candidates[0].content.parts && gemJson.candidates[0].content.parts[0] && gemJson.candidates[0].content.parts[0].text) || '')
          .trim()
          .replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/i, '').trim();
        if (rawText) {
          try {
            geminiData = JSON.parse(rawText);
          } catch (parseErr) {
            // Grounded responses sometimes include trailing citation text
            // after the JSON object. Try to extract the first {...} block.
            const start = rawText.indexOf('{');
            const end   = rawText.lastIndexOf('}');
            if (start !== -1 && end > start) {
              try { geminiData = JSON.parse(rawText.slice(start, end + 1)); }
              catch (e2) { console.warn('[Gemini] JSON extract failed: ' + e2.message); }
            } else {
              console.warn('[Gemini] JSON parse failed: ' + parseErr.message);
            }
          }
        }
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
      client_name: d.client_name || '',
      custom_location_notes: d.custom_location_notes,
    }, analysisResult, {
      audience: d.audience || 'neutral',
      exec_summary: geminiData.exec_summary || '',
      metrics_narrative: geminiData.metrics_narrative || '',
      volume_narrative: geminiData.volume_narrative || '',
      price_narrative: geminiData.price_narrative || '',
      market_outlook_narrative: geminiData.market_outlook_narrative || '',
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
