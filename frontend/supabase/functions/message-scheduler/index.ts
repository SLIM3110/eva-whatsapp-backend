import { serve } from 'https://deno.land/std@0.168.0/http/server.ts'
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

serve(async () => {
  const supabase = createClient(
    Deno.env.get('SUPABASE_URL') ?? '',
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
  )
  const { data } = await supabase
    .from('api_settings')
    .select('whatsapp_backend_url, whatsapp_api_key')
    .eq('id', 1)
    .single()
  if (!data?.whatsapp_backend_url) {
    return new Response('No backend URL configured', { status: 200 })
  }
  await fetch(`${data.whatsapp_backend_url}/api/health/trigger`, {
    method: 'POST',
    headers: { 'x-api-key': data.whatsapp_api_key }
  })
  return new Response('Triggered', { status: 200 })
})
