require('dotenv').config();

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const WEBHOOK_URL = 'https://api.evaintelligencehub.online/webhook/incoming';

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('SUPABASE_URL and SUPABASE_SERVICE_KEY must be set');
  process.exit(1);
}

async function fetchProfiles() {
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/profiles?green_api_instance_id=not.is.null&green_api_token=not.is.null&select=id,green_api_instance_id,green_api_token,green_api_url`,
    {
      headers: {
        'apikey': SUPABASE_SERVICE_KEY,
        'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`
      }
    }
  );
  if (!res.ok) throw new Error(`Failed to fetch profiles: ${await res.text()}`);
  return res.json();
}

async function registerWebhook(profile) {
  const { id, green_api_instance_id, green_api_token, green_api_url } = profile;
  const apiUrl = green_api_url || 'https://api.green-api.com';

  const res = await fetch(`${apiUrl}/waInstance${green_api_instance_id}/setSettings/${green_api_token}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      webhookUrl: WEBHOOK_URL,
      webhookUrlToken: '',
      incomingWebhook: 'yes'
    })
  });

  if (!res.ok) {
    const body = await res.text();
    console.error(`  FAIL  profile=${id} instance=${green_api_instance_id}: ${body}`);
  } else {
    console.log(`  OK    profile=${id} instance=${green_api_instance_id}`);
  }
}

(async () => {
  console.log(`Backfilling webhooks → ${WEBHOOK_URL}\n`);
  const profiles = await fetchProfiles();
  console.log(`Found ${profiles.length} profile(s) with Green API credentials\n`);

  for (const profile of profiles) {
    await registerWebhook(profile);
  }

  console.log('\nDone.');
})();
