'use strict';

/**
 * Email Campaigns — Resend integration
 *
 * Endpoints:
 *   GET    /api/email/audiences
 *   POST   /api/email/audiences
 *   GET    /api/email/audiences/:id/contacts
 *   POST   /api/email/audiences/:id/sync-contacts   — pull from owner_contacts table
 *   GET    /api/email/broadcasts
 *   POST   /api/email/broadcasts
 *   POST   /api/email/broadcasts/:id/send
 */

const express = require('express');
const router = express.Router();
const resend = require('../services/resend');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// Auth middleware — same pattern as other routes
const auth = (req, res, next) => {
  const key = req.headers['x-api-key'];
  const expected = process.env.WHATSAPP_API_KEY || process.env.API_SECRET_KEY;
  if (!expected || key !== expected) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
};

router.use(auth);

// ── Audiences ──────────────────────────────────────────────

router.get('/audiences', async (req, res) => {
  try {
    const audiences = await resend.listAudiences();
    res.json({ audiences });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.post('/audiences', async (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'name is required' });
  try {
    const audience = await resend.createAudience(name);
    res.json({ audience });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/audiences/:id/contacts', async (req, res) => {
  try {
    const contacts = await resend.listContacts(req.params.id);
    res.json({ contacts });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Sync owner_contacts from Supabase into a Resend audience
router.post('/audiences/:id/sync-contacts', async (req, res) => {
  const audienceId = req.params.id;
  try {
    const { data: contacts, error } = await supabase
      .from('owner_contacts')
      .select('owner_email, owner_name')
      .not('owner_email', 'is', null)
      .neq('owner_email', '');

    if (error) throw new Error(error.message);

    let synced = 0;
    let skipped = 0;

    for (const c of contacts || []) {
      const email = (c.owner_email || '').trim().toLowerCase();
      if (!email) { skipped++; continue; }

      const nameParts = (c.owner_name || '').trim().split(' ');
      const firstName = nameParts[0] || '';
      const lastName = nameParts.slice(1).join(' ') || '';

      try {
        await resend.addContact(audienceId, email, firstName, lastName);
        synced++;
      } catch (e) {
        // skip duplicates or invalid emails silently
        skipped++;
      }
    }

    res.json({ success: true, synced, skipped });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Broadcasts ─────────────────────────────────────────────

router.get('/broadcasts', async (req, res) => {
  try {
    const broadcasts = await resend.listBroadcasts();
    res.json({ broadcasts });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.post('/broadcasts', async (req, res) => {
  const { name, from, replyTo, subject, html, audienceId } = req.body;
  if (!name || !from || !subject || !html || !audienceId) {
    return res.status(400).json({ error: 'name, from, subject, html, and audienceId are required' });
  }
  try {
    const broadcast = await resend.createBroadcast({ name, from, replyTo, subject, html, audienceId });
    res.json({ broadcast });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.post('/broadcasts/:id/send', async (req, res) => {
  const { scheduledAt } = req.body;
  try {
    const result = await resend.sendBroadcast(req.params.id, scheduledAt);
    res.json({ success: true, result });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/broadcasts/:id', async (req, res) => {
  try {
    const broadcast = await resend.getBroadcast(req.params.id);
    res.json({ broadcast });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
