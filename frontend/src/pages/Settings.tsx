import { useState, useEffect } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { toast } from 'sonner';
import { Loader2, Eye, EyeOff, Check, Wifi, WifiOff } from 'lucide-react';

const SettingsPage = () => {
  const [whatsappUrl, setWhatsappUrl] = useState('');
  const [whatsappKey, setWhatsappKey] = useState('');
  const [geminiKey, setGeminiKey] = useState('');
  const [showWhatsappKey, setShowWhatsappKey] = useState(false);
  const [showGeminiKey, setShowGeminiKey] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<'connected' | 'failed' | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetch = async () => {
      const { data } = await supabase.from('api_settings').select('*').eq('id', 1).single();
      if (data) {
        setWhatsappUrl(data.whatsapp_backend_url || '');
        setWhatsappKey(data.whatsapp_api_key || '');
        setGeminiKey(data.gemini_api_key || '');
      }
      setLoading(false);
    };
    fetch();
  }, []);

  const saveWhatsapp = async () => {
    setSaving(true);
    const { error } = await supabase.from('api_settings').update({
      whatsapp_backend_url: whatsappUrl,
      whatsapp_api_key: whatsappKey,
    }).eq('id', 1);
    if (error) toast.error('Failed to save');
    else toast.success('WhatsApp settings saved');
    setSaving(false);
  };

  const testConnection = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const res = await fetch(`${whatsappUrl}/api/health`, { headers: { 'x-api-key': whatsappKey } });
      const data = await res.json();
      setTestResult(data.status === 'ok' ? 'connected' : 'failed');
    } catch {
      setTestResult('failed');
    }
    setTesting(false);
  };

  const saveGemini = async () => {
    setSaving(true);
    const { error } = await supabase.from('api_settings').update({ gemini_api_key: geminiKey }).eq('id', 1);
    if (error) toast.error('Failed to save');
    else toast.success('Gemini API key saved');
    setSaving(false);
  };

  if (loading) return <div className="flex items-center justify-center py-20"><Loader2 className="animate-spin w-8 h-8 text-primary" /></div>;

  return (
    <div className="space-y-8 max-w-2xl">
      <Card>
        <CardHeader><CardTitle>WhatsApp Backend</CardTitle></CardHeader>
        <CardContent className="space-y-4">
          <div>
            <label className="text-sm font-medium">WhatsApp Backend URL</label>
            <Input value={whatsappUrl} onChange={(e) => setWhatsappUrl(e.target.value)} placeholder="https://your-whatsapp-backend.com" className="mt-1" />
          </div>
          <div>
            <label className="text-sm font-medium">WhatsApp API Key</label>
            <div className="flex gap-2 mt-1">
              <Input type={showWhatsappKey ? 'text' : 'password'} value={whatsappKey} onChange={(e) => setWhatsappKey(e.target.value)} />
              <Button variant="ghost" size="icon" onClick={() => setShowWhatsappKey(!showWhatsappKey)}>
                {showWhatsappKey ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </Button>
            </div>
          </div>
          <div className="flex gap-2">
            <Button onClick={saveWhatsapp} disabled={saving}>
              {saving ? <Loader2 className="animate-spin mr-2" /> : <Check className="w-4 h-4 mr-2" />} Save
            </Button>
            <Button variant="outline" onClick={testConnection} disabled={testing || !whatsappUrl}>
              {testing ? <Loader2 className="animate-spin mr-2" /> : null} Test Connection
            </Button>
          </div>
          {testResult && (
            <div className={`flex items-center gap-2 text-sm font-medium ${testResult === 'connected' ? 'text-green-600' : 'text-destructive'}`}>
              {testResult === 'connected' ? <Wifi className="w-4 h-4" /> : <WifiOff className="w-4 h-4" />}
              {testResult === 'connected' ? 'Connected' : 'Failed'}
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle>Gemini API</CardTitle></CardHeader>
        <CardContent className="space-y-4">
          <div>
            <label className="text-sm font-medium">Gemini API Key</label>
            <div className="flex gap-2 mt-1">
              <Input type={showGeminiKey ? 'text' : 'password'} value={geminiKey} onChange={(e) => setGeminiKey(e.target.value)} />
              <Button variant="ghost" size="icon" onClick={() => setShowGeminiKey(!showGeminiKey)}>
                {showGeminiKey ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </Button>
            </div>
          </div>
          <Button onClick={saveGemini} disabled={saving}>
            {saving ? <Loader2 className="animate-spin mr-2" /> : <Check className="w-4 h-4 mr-2" />} Save
          </Button>
        </CardContent>
      </Card>
    </div>
  );
};

export default SettingsPage;
