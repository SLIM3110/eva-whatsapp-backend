'use strict';

const { Resend } = require('resend');

function getClient() {
  if (!process.env.RESEND_API_KEY) {
    throw new Error('RESEND_API_KEY not configured — add it to .env and restart');
  }
  return new Resend(process.env.RESEND_API_KEY);
}

async function listAudiences() {
  const r = getClient();
  const { data, error } = await r.audiences.list();
  if (error) throw new Error(error.message || 'Failed to list audiences');
  return data || [];
}

async function createAudience(name) {
  const r = getClient();
  const { data, error } = await r.audiences.create({ name });
  if (error) throw new Error(error.message || 'Failed to create audience');
  return data;
}

async function listContacts(audienceId) {
  const r = getClient();
  const { data, error } = await r.contacts.list({ audienceId });
  if (error) throw new Error(error.message || 'Failed to list contacts');
  return data || [];
}

async function addContact(audienceId, email, firstName, lastName) {
  const r = getClient();
  const { data, error } = await r.contacts.create({
    audienceId,
    email,
    firstName: firstName || '',
    lastName: lastName || '',
    unsubscribed: false,
  });
  if (error && error.statusCode !== 409) {
    // 409 = already exists, skip silently
    throw new Error(error.message || 'Failed to add contact');
  }
  return data;
}

async function listBroadcasts() {
  const r = getClient();
  const { data, error } = await r.broadcasts.list();
  if (error) throw new Error(error.message || 'Failed to list broadcasts');
  return data || [];
}

async function createBroadcast(opts) {
  const r = getClient();
  const payload = {
    name: opts.name,
    from: opts.from,
    subject: opts.subject,
    html: opts.html,
    audienceId: opts.audienceId,
  };
  if (opts.replyTo) payload.replyTo = opts.replyTo;
  const { data, error } = await r.broadcasts.create(payload);
  if (error) throw new Error(error.message || 'Failed to create broadcast');
  return data;
}

async function sendBroadcast(id, scheduledAt) {
  const r = getClient();
  const opts = scheduledAt ? { scheduledAt } : {};
  const { data, error } = await r.broadcasts.send(id, opts);
  if (error) throw new Error(error.message || 'Failed to send broadcast');
  return data;
}

async function getBroadcast(id) {
  const r = getClient();
  const { data, error } = await r.broadcasts.get(id);
  if (error) throw new Error(error.message || 'Failed to get broadcast');
  return data;
}

module.exports = {
  listAudiences,
  createAudience,
  listContacts,
  addContact,
  listBroadcasts,
  createBroadcast,
  sendBroadcast,
  getBroadcast,
};
