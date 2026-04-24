/**
 * aiVariation.js
 * ──────────────
 * Gemini-powered message variation service for EVA WhatsApp outreach.
 *
 * Design goals:
 *   - Vary wording enough that each message has a unique fingerprint
 *   - Never alter proper nouns (agent names, building names, community names,
 *     unit numbers) or structural markers (bullet points, line breaks)
 *   - Validate output strictly; fall back to local variation on any failure
 *   - Background latency only — called at send time, not upload time
 *   - Cost: ~$1–2/month at 750 messages/month with Gemini 2.0 Flash
 *
 * Usage:
 *   const { varyMessage } = require('./services/aiVariation');
 *   const finalText = await varyMessage(contact.generated_message);
 */

'use strict';

// ── Config ────────────────────────────────────────────────────────────────────

const GEMINI_MODEL   = 'gemini-1.5-flash';
const GEMINI_TIMEOUT = 18000; // ms
const MAX_RETRIES    = 2;     // attempts before falling back to local

// Length ratio bounds — AI output must stay within these relative to input
const MIN_RATIO = 0.75;
const MAX_RATIO = 1.35;

// ── Prompt ────────────────────────────────────────────────────────────────────

function buildPrompt(message) {
  return `You are a message variation assistant for EVA Real Estate agents in Dubai.
Your task: lightly rewrite the outreach message below so it reads naturally but differs enough in wording from other sends in the same batch to avoid WhatsApp spam fingerprinting.

STRICT RULES — violating any rule means the output is rejected:
1. Do NOT change any proper nouns: agent names, owner names, building names, community names, tower names, unit numbers, company names (e.g. EVA Real Estate). Keep them character-for-character identical.
2. Do NOT add, remove, or reorder bullet points or numbered lists. If the original has bullets, the output must have the same number of bullets.
3. Do NOT change the core meaning or the call to action. The intent must remain identical.
4. Do NOT remove or change any greeting (e.g. "Dear Ahmed") or sign-off (agent name + title).
5. Keep the output within 75%–135% of the original character length.
6. Do NOT add new facts, prices, dates, or statistics not in the original.
7. Output ONLY the rewritten message text — no labels, no commentary, no quotes around the message.

Allowed variations:
- Swap synonyms (e.g. "assist" → "help", "property" → "unit", "reach out" → "get in touch")
- Lightly rephrase a sentence (same meaning, different word order)
- Vary punctuation style (em-dash vs comma, short sentence vs joined clause)
- Adjust formality slightly (contractions on/off)

Message to vary:

${message}`;
}

// ── Local fallback variation (no AI, deterministic) ───────────────────────────

const _hash = (n, salt) => {
  let h = (((n * 2654435761) >>> 0) ^ ((salt * 40503) >>> 0)) >>> 0;
  h = (((h >>> 16) ^ h) * 1664525) >>> 0;
  h = ((h >>> 16) ^ h) >>> 0;
  return h;
};
const _pick = (arr, index, salt) => arr[_hash(index, salt) % arr.length];
const _chance = (index, salt, pct) => (_hash(index, salt) % 100) < pct;

const GREETING_SWAPS = [
  [/^(Dear)\b/im,  ['Dear', 'Hello', 'Hi', 'Good day']],
  [/^(Hello)\b/im, ['Hello', 'Hi', 'Dear', 'Good day']],
  [/^(Hi)\b/im,    ['Hi', 'Hello', 'Dear']],
];

const SYNONYM_SWAPS = [
  [/\bassist\b/gi,         ['assist', 'help', 'support']],
  [/\bproperty\b/gi,       ['property', 'unit', 'home', 'residence']],
  [/\breach out\b/gi,      ['reach out', 'get in touch', 'contact me', 'drop me a message']],
  [/\bget in touch\b/gi,   ['get in touch', 'reach out', 'contact me', 'connect']],
  [/\bhappy to\b/gi,       ['happy to', 'glad to', 'pleased to', 'delighted to']],
  [/\bkindly\b/gi,         ['kindly', 'please', 'do']],
  [/\bopportunity\b/gi,    ['opportunity', 'chance', 'option']],
  [/\bcurrently\b/gi,      ['currently', 'at the moment', 'right now', 'at present']],
  [/\bexplore\b/gi,        ['explore', 'discuss', 'look into', 'consider']],
  [/\bshare\b/gi,          ['share', 'provide', 'send', 'pass on']],
  [/\bmarket\b/gi,         ['market', 'sector', 'landscape']],
  [/\bpotential\b/gi,      ['potential', 'possible', 'prospective']],
  [/\bsignificant\b/gi,    ['significant', 'notable', 'considerable', 'meaningful']],
  [/\butilise\b/gi,        ['utilise', 'use', 'leverage', 'take advantage of']],
  [/\bfurther\b/gi,        ['further', 'additional', 'more']],
  [/\bensure\b/gi,         ['ensure', 'make sure', 'guarantee']],
  [/\bspecialise\b/gi,     ['specialise', 'focus', 'work']],
  [/\bvaluable\b/gi,       ['valuable', 'useful', 'helpful', 'worthwhile']],
  [/\bdetails\b/gi,        ['details', 'information', 'specifics']],
  [/\bconvenient\b/gi,     ['convenient', 'suitable', 'good']],
];

const CONTRACTIONS_EXPAND = [
  [/\bI'm\b/g, "I am"], [/\bI've\b/g, "I have"], [/\bI'll\b/g, "I will"],
  [/\bwe're\b/g, "we are"], [/\bwe've\b/g, "we have"], [/\bwe'll\b/g, "we will"],
  [/\bdon't\b/g, "do not"], [/\bdoesn't\b/g, "does not"], [/\bcan't\b/g, "cannot"],
  [/\bwon't\b/g, "will not"], [/\bit's\b/g, "it is"], [/\bthat's\b/g, "that is"],
];
const CONTRACTIONS_CONTRACT = [
  [/\bI am\b/g, "I'm"], [/\bI have\b/g, "I've"], [/\bI will\b/g, "I'll"],
  [/\bwe are\b/g, "we're"], [/\bwe have\b/g, "we've"], [/\bwe will\b/g, "we'll"],
  [/\bdo not\b/g, "don't"], [/\bdoes not\b/g, "doesn't"], [/\bcannot\b/g, "can't"],
  [/\bwill not\b/g, "won't"], [/\bit is\b/g, "it's"], [/\bthat is\b/g, "that's"],
];

const PS_LINES = [
  'P.S. If you would like a free valuation of your unit, I am happy to arrange one — no cost, no obligation.',
  'P.S. Happy to share recent comparable sales in your building if that would be useful.',
  'P.S. If you are simply curious about what your unit is worth today, I can give you a quick overview.',
  'P.S. I can share recent market data specific to your community if you are interested.',
  'P.S. Even if you are not looking to make a move right now, knowing your property value is always useful.',
  'P.S. A quick 5-minute call is all it takes — no pressure, no obligation.',
  'P.S. I work with a number of owners in your building and would be glad to share insights.',
  'P.S. If timing is not right for you now, I am happy to stay in touch for when it is.',
  'P.S. We have a strong network of qualified buyers actively looking in this community right now.',
  'P.S. I have helped several owners in your building navigate both rentals and sales — happy to share more.',
  'P.S. Feel free to save my number for whenever the timing works for you.',
  'P.S. No catch — just a genuine offer to help you understand your options.',
];

const CLOSING_VARIANTS = [
  'Looking forward to connecting with you.',
  'I look forward to hearing from you.',
  'Please feel free to reach out at any time.',
  'Do not hesitate to get in touch.',
  'Happy to answer any questions you may have.',
  'Hope to hear from you soon.',
  'Feel free to message me anytime.',
  'Happy to have a quick chat whenever suits you.',
  'I look forward to speaking with you.',
  'Reach out anytime — happy to help.',
  'I hope to connect with you soon.',
  'Do reach out whenever you are ready.',
  'Looking forward to the opportunity to assist you.',
  'Always happy to have a no-pressure conversation.',
];

// Seed derived from the message content for determinism within a send batch
function msgSeed(message) {
  let h = 0;
  for (let i = 0; i < message.length; i++) {
    h = (Math.imul(31, h) + message.charCodeAt(i)) >>> 0;
  }
  return h % 100000;
}

function localVariation(message) {
  const index = msgSeed(message);
  let varied = message;

  // 1. Greeting
  for (const [pat, alts] of GREETING_SWAPS) {
    if (pat.test(varied)) {
      varied = varied.replace(pat, _pick(alts, index, 0));
      break;
    }
  }

  // 2. Synonym swaps
  SYNONYM_SWAPS.forEach(([pat, alts], salt) => {
    if (pat.test(varied)) varied = varied.replace(pat, _pick(alts, index, salt + 1));
  });

  // 3. Contraction mode
  if (index % 2 === 0) {
    CONTRACTIONS_EXPAND.forEach(([p, r]) => { varied = varied.replace(p, r); });
  } else {
    CONTRACTIONS_CONTRACT.forEach(([p, r]) => { varied = varied.replace(p, r); });
  }

  // 4. Closing if absent
  const hasClosing = /looking forward|feel free|don.?t hesitate|reach out|get in touch|happy to|hear from you|message me|speak with you|here whenever|stay in touch/i.test(varied);
  if (!hasClosing) {
    varied = varied.trimEnd() + '\n\n' + _pick(CLOSING_VARIANTS, index, 11);
  }

  // 5. P.S. on ~45% of messages
  if (_chance(index, 99, 45)) {
    varied = varied.trimEnd() + '\n\n' + _pick(PS_LINES, index, 77);
  }

  // 6. Em-dash alternation
  if (index % 2 === 0) {
    varied = varied.replace(/ — /g, ' - ');
  } else {
    varied = varied.replace(/ - /g, ' — ');
  }

  return varied;
}

// ── Validation ────────────────────────────────────────────────────────────────

function validate(original, output) {
  if (!output || typeof output !== 'string' || output.trim().length === 0) {
    return { ok: false, reason: 'empty output' };
  }

  const ratio = output.length / original.length;
  if (ratio < MIN_RATIO || ratio > MAX_RATIO) {
    return { ok: false, reason: `length ratio ${ratio.toFixed(2)} out of bounds [${MIN_RATIO}, ${MAX_RATIO}]` };
  }

  // No remaining unfilled placeholders
  if (/\{\{[^}]+\}\}/.test(output)) {
    return { ok: false, reason: 'unfilled placeholder detected in output' };
  }

  // Bullet count must match
  const origBullets = (original.match(/^\s*[•\-\*]\s/gm) || []).length;
  const outBullets  = (output.match(/^\s*[•\-\*]\s/gm) || []).length;
  if (origBullets > 0 && outBullets !== origBullets) {
    return { ok: false, reason: `bullet count changed (${origBullets} → ${outBullets})` };
  }

  return { ok: true };
}

// ── Main export ───────────────────────────────────────────────────────────────

/**
 * varyMessage(message)
 * Returns a varied version of the message.
 * Always returns a string — falls back to local variation if AI fails.
 */
async function varyMessage(message) {
  const geminiKey = process.env.GEMINI_API_KEY;

  if (!geminiKey) {
    console.warn('[aiVariation] GEMINI_API_KEY not set — using local variation');
    return localVariation(message);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    let aiOutput = null;

    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), GEMINI_TIMEOUT);

      const res = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${geminiKey}`,
        {
          method:  'POST',
          signal:  controller.signal,
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({
            contents: [{ parts: [{ text: buildPrompt(message) }] }],
            generationConfig: { temperature: 0.65, maxOutputTokens: 1024 },
          }),
        }
      );
      clearTimeout(timer);

      if (res.status === 429) {
        console.warn(`[aiVariation] Gemini 429 rate limit on attempt ${attempt}`);
        if (attempt < MAX_RETRIES) {
          await new Promise(r => setTimeout(r, 3000 * attempt));
          continue;
        }
        break;
      }

      if (!res.ok) {
        const body = await res.text();
        console.warn(`[aiVariation] Gemini HTTP ${res.status} on attempt ${attempt}: ${body.slice(0, 200)}`);
        break;
      }

      const data = await res.json();
      aiOutput = data?.candidates?.[0]?.content?.parts?.[0]?.text?.trim() ?? null;

    } catch (err) {
      console.warn(`[aiVariation] Gemini request error on attempt ${attempt}:`, err.message);
      if (attempt >= MAX_RETRIES) break;
      await new Promise(r => setTimeout(r, 2000));
      continue;
    }

    const { ok, reason } = validate(message, aiOutput);
    if (ok) {
      console.log(`[aiVariation] AI variation succeeded (attempt ${attempt})`);
      return aiOutput;
    } else {
      console.warn(`[aiVariation] Validation failed (attempt ${attempt}): ${reason}`);
    }
  }

  // All attempts failed — fall back to local variation
  console.warn('[aiVariation] Falling back to local variation');
  return localVariation(message);
}

module.exports = { varyMessage, localVariation };
