'use strict';

const express = require('express');
const router  = express.Router();

const FETCH_TIMEOUT_MS  = 10000;
const GEMINI_TIMEOUT_MS = 18000;

// ── Supabase helper ───────────────────────────────────────────────────────────

async function supabaseFetch(path, options = {}) {
  const baseUrl    = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;

  const controller = new AbortController();
  const timer      = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const res = await fetch(`${baseUrl}/rest/v1${path}`, {
      ...options,
      signal: controller.signal,
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
  } finally {
    clearTimeout(timer);
  }
}

// ── Green API send helper ─────────────────────────────────────────────────────

async function sendViaGreenApi(agentCreds, toNumber, message) {
  const { green_api_url, green_api_instance_id, green_api_token } = agentCreds;
  const chatId = `${toNumber.replace(/\D/g, '')}@c.us`;
  try {
    const res = await fetch(
      `${green_api_url}/waInstance${green_api_instance_id}/sendMessage/${green_api_token}`,
      {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ chatId, message }),
      }
    );
    if (!res.ok) {
      const body = await res.text();
      console.error(`[webhook/send] Green API error: ${body.slice(0, 200)}`);
      return false;
    }
    return true;
  } catch (e) {
    console.error('[webhook/send] Error:', e.message);
    return false;
  }
}

// ── Gemini personalised reply generator ──────────────────────────────────────

async function generateReply(originalMessage, leadReply, agentFirstName, geminiKey) {
  try {
    const controller = new AbortController();
    const timeoutId  = setTimeout(() => controller.abort(), GEMINI_TIMEOUT_MS);

    const prompt =
`You are ${agentFirstName}, a real estate agent at EVA Real Estate in Dubai.

You sent this WhatsApp outreach message to a property owner:
"""
${originalMessage}
"""

The property owner replied:
"""
${leadReply}
"""

Write a short (2–4 sentence), warm, natural follow-up reply that:
- Acknowledges specifically what they said
- Moves the conversation forward (suggest a quick call or ask one relevant question)
- Sounds like a real person, not a bot — conversational, not formal
- Does NOT use hollow phrases like "Great to hear from you!" or "I hope this finds yo