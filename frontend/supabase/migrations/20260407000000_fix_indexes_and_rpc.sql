-- ============================================================
-- Migration: Add missing indexes and fix increment_batch_sent RPC
-- ============================================================

-- Indexes for owner_contacts
CREATE INDEX IF NOT EXISTS idx_owner_contacts_assigned_agent
  ON public.owner_contacts (assigned_agent);

CREATE INDEX IF NOT EXISTS idx_owner_contacts_message_status
  ON public.owner_contacts (message_status);

CREATE INDEX IF NOT EXISTS idx_owner_contacts_agent_status
  ON public.owner_contacts (assigned_agent, message_status);

-- Indexes for messages_log
CREATE INDEX IF NOT EXISTS idx_messages_log_agent_id
  ON public.messages_log (agent_id);

CREATE INDEX IF NOT EXISTS idx_messages_log_sent_at
  ON public.messages_log (sent_at);

CREATE INDEX IF NOT EXISTS idx_messages_log_agent_sent_at
  ON public.messages_log (agent_id, sent_at);

-- Index for profiles scheduler query (role + status + is_active)
CREATE INDEX IF NOT EXISTS idx_profiles_scheduler
  ON public.profiles (whatsapp_session_status, is_active, role);

-- ============================================================
-- Fix increment_batch_sent RPC
-- Increments sent_count AND decrements pending_count atomically
-- ============================================================
CREATE OR REPLACE FUNCTION public.increment_batch_sent(p_batch_id UUID)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  UPDATE public.batches
  SET
    sent_count    = sent_count + 1,
    pending_count = GREATEST(0, pending_count - 1)
  WHERE id = p_batch_id;
END;
$$;
