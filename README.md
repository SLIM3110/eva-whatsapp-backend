# EVA Intelligence Hub

WhatsApp automation platform for EVA Real Estate Dubai.
Sends AI-personalised messages from each agent's personal WhatsApp to their assigned property owner contacts — 50 per day, randomised between 9am and 7pm UAE time.

## Repository Structure

```
EVA-intelligence-hub/
  backend/          Node.js + Express + Baileys WhatsApp (Railway)
    src/
      server.js         Entry point
      scheduler.js      Message sending scheduler (runs every 60s)
      sessionManager.js WhatsApp session management via Baileys
      routes/
        health.js       Health check + manual scheduler trigger
        session.js      QR, connect, disconnect, pause, resume
        message.js      Direct message sending
    package.json
    Procfile          Railway start command
    .env.example      Environment variable template

  frontend/         React + TypeScript + Vite (Lovable / Vercel)
    src/
      pages/            Route-level components
      components/       Shared UI components
      contexts/         Auth context
      integrations/     Supabase client
      lib/              Utilities
    supabase/
      migrations/       Database schema migrations
      functions/        Edge functions
    package.json
    .env.example      Environment variable template
```

## Backend Setup (Railway)

1. In Railway, set **Root Directory** to `backend`
2. Set these environment variables in Railway:

| Variable | Value |
|---|---|
| `WHATSAPP_API_KEY` | Secret key for API auth |
| `SUPABASE_URL` | `https://guwmfmwyqrwvufchkzfc.supabase.co` |
| `SUPABASE_SERVICE_KEY` | Service role key from Supabase dashboard |
| `GEMINI_API_KEY` | Google Gemini API key |

### API Endpoints
All require header `x-api-key` except `/api/health`.

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/health` | Health check |
| POST | `/api/health/trigger` | Trigger one scheduler tick manually |
| POST | `/api/session/start` | Start session, get QR code |
| GET | `/api/session/status?agentId=` | Get session status |
| POST | `/api/session/disconnect` | Disconnect and clear session |
| POST | `/api/session/pause` | Pause sending |
| POST | `/api/session/resume` | Resume sending |

## Frontend Setup

```bash
cd frontend
cp .env.example .env.local
# Add your Supabase URL and anon key to .env.local
npm install
npm run dev
```

| Variable | Value |
|---|---|
| `VITE_SUPABASE_URL` | `https://guwmfmwyqrwvufchkzfc.supabase.co` |
| `VITE_SUPABASE_ANON_KEY` | Anon key from Supabase dashboard |

## Database (Supabase)

Project ID: `guwmfmwyqrwvufchkzfc`
Migrations: `frontend/supabase/migrations/`

| Table | Purpose |
|---|---|
| `profiles` | Users with roles, WhatsApp status, active flag |
| `owner_contacts` | Property owner contacts with assigned agents |
| `batches` | CSV upload batches |
| `message_templates` | WhatsApp message templates with placeholders |
| `messages_log` | Full log of all sent messages |
| `api_settings` | Single-row config (backend URL, API keys) |
| `user_roles` | Role assignments for RLS policies |
