import { useState, useCallback, useEffect, useRef } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { useAuth } from '@/contexts/AuthContext';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Progress } from '@/components/ui/progress';
import { toast } from 'sonner';
import { toUAETime } from '@/lib/uaeTime';
import { Upload, Loader2, Eye, ArrowLeft, QrCode, Pause, Play } from 'lucide-react';

const UnitCollector = () => {
  const { user, profile, refreshProfile } = useAuth();
  const [batchName, setBatchName] = useState('');
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [templates, setTemplates] = useState<any[]>([]);
  const [selectedTemplate, setSelectedTemplate] = useState('');
  const [agents, setAgents] = useState<any[]>([]);
  const [uploading, setUploading] = useState(false);
  const [batches, setBatches] = useState<any[]>([]);
  const [viewingBatch, setViewingBatch] = useState<string | null>(null);
  const [contacts, setContacts] = useState<any[]>([]);
  const [filterStatus, setFilterStatus] = useState('');
  const [filterAgent, setFilterAgent] = useState('');
  const [loading, setLoading] = useState(true);
  const [expandedMsg, setExpandedMsg] = useState<string | null>(null);
  const [qrCode, setQrCode] = useState<string | null>(null);
  const [connectingWhatsApp, setConnectingWhatsApp] = useState(false);
  const [sendingPaused, setSendingPaused] = useState(false);
  const [togglingPause, setTogglingPause] = useState(false);
  const pollingRef = useRef<number | null>(null);

  const isAdmin = profile?.role === 'super_admin' || profile?.role === 'admin';
  const isConnected = profile?.whatsapp_session_status === 'connected';

  const clearPolling = useCallback(() => {
    if (pollingRef.current) {
      window.clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  }, []);

  const fetchData = useCallback(async () => {
    const promises: any[] = [
      supabase.from('message_templates').select('*'),
      isAdmin
        ? supabase.from('batches').select('*').order('upload_date', { ascending: false })
        : supabase.from('batches').select('*').eq('uploaded_by', user!.id).order('upload_date', { ascending: false }),
      supabase.from('profiles').select('id, first_name, last_name, role'),
      supabase.from('profiles').select('sending_paused').eq('id', user!.id).single(),
    ];

    const [templatesRes, batchesRes, allProfilesRes, pauseRes] = await Promise.all(promises);
    setTemplates(templatesRes.data || []);
    setSendingPaused(pauseRes.data?.sending_paused ?? false);
    const allProfiles = allProfilesRes.data || [];
    setAgents(allProfiles.filter((p: any) => p.role === 'agent'));
    const profileMap = Object.fromEntries(allProfiles.map((p: any) => [p.id, p]));
    setBatches((batchesRes.data || []).map((b: any) => ({
      ...b,
      uploader: profileMap[b.uploaded_by] || null,
    })));
    const defaultTpl = templatesRes.data?.find((t: any) => t.is_default);
    if (defaultTpl) setSelectedTemplate(defaultTpl.id);
    setLoading(false);
  }, [isAdmin, user]);

  useEffect(() => { fetchData(); }, [fetchData]);

  useEffect(() => () => clearPolling(), [clearPolling]);

  const PHONE_KEYWORDS = ['mobile', 'phone', 'number', 'tel', 'contact', 'whatsapp', 'cell', 'mob'];
  const PHONE_EXACT = ['mobile1','mobile2','mobile3','mobile_1','mobile_2','mobile_3','mobile 1','mobile 2','mobile 3'];

  const isPhoneColumn = (header: string) => {
    const h = header.toLowerCase().replace(/[_\s]/g, '');
    if (PHONE_EXACT.map(e => e.replace(/[_\s]/g, '')).includes(h)) return true;
    return PHONE_KEYWORDS.some(kw => header.toLowerCase().includes(kw));
  };

  const cleanPhone = (raw: string): string | null => {
    if (!raw) return null;
    let num = raw.replace(/[\s\-\(\)\+]/g, '');
    if (!/^\d+$/.test(num)) return null;
    if (num.startsWith('0')) num = '971' + num.slice(1);
    if (num.startsWith('5') && num.length === 9) num = '971' + num;
    return num.length >= 10 ? num : null;
  };

  const parseCSV = (text: string) => {
    const lines = text.trim().split('\n');
    const rawHeaders = lines[0].split(',').map(h => h.trim());
    const headers = rawHeaders.map(h => h.toLowerCase());

    const phoneColIndexes = headers.map((h, i) => isPhoneColumn(h) ? i : -1).filter(i => i >= 0);
    if (phoneColIndexes.length === 0) {
      throw new Error('No phone number columns found. Your CSV must have at least one column with mobile, phone, or number in the header name.');
    }

    const nameColIdx = headers.findIndex(h => h.includes('name') || h.includes('owner'));
    const buildingColIdx = headers.findIndex(h => h.includes('building') || h.includes('tower') || h.includes('property'));
    const unitColIdx = headers.findIndex(h => h.includes('unit') || h.includes('apartment') || h.includes('flat'));

    const dataRows = lines.slice(1).filter(l => l.trim());
    const contacts: { owner_name: string; building_name: string; unit_number: string; phone: string }[] = [];

    for (const line of dataRows) {
      const vals = line.split(',').map(v => v.trim());
      const ownerName = nameColIdx >= 0 ? (vals[nameColIdx] || 'Unknown') : 'Unknown';
      const buildingName = buildingColIdx >= 0 ? (vals[buildingColIdx] || '') : '';
      const unitNumber = unitColIdx >= 0 ? (vals[unitColIdx] || '') : '';

      for (const idx of phoneColIndexes) {
        const cleaned = cleanPhone(vals[idx] || '');
        if (cleaned) {
          contacts.push({ owner_name: ownerName, building_name: buildingName, unit_number: unitNumber, phone: cleaned });
        }
      }
    }

    return contacts;
  };

  const substituteTemplate = (template: string, row: any, agentName: string) => {
    return template
      .replace(/\{\{owner_name\}\}/g, row.owner_name || '')
      .replace(/\{\{building_name\}\}/g, row.building_name || '')
      .replace(/\{\{unit_number\}\}/g, row.unit_number || '')
      .replace(/\{\{agent_first_name\}\}/g, agentName);
  };

  const pollWhatsAppStatus = useCallback((url: string, key: string) => {
    clearPolling();
    pollingRef.current = window.setInterval(async () => {
      try {
        const res = await fetch(`${url}/api/session/status?agentId=${user?.id}`, {
          headers: { 'x-api-key': key },
        });
        const data = await res.json();

        if (data.status === 'connected') {
          clearPolling();
          setQrCode(null);
          setConnectingWhatsApp(false);
          await supabase.from('profiles').update({ whatsapp_session_status: 'connected' }).eq('id', user?.id);
          await refreshProfile();
          toast.success('WhatsApp connected');
        }
      } catch {
        clearPolling();
        setConnectingWhatsApp(false);
        toast.error('Failed while checking WhatsApp status');
      }
    }, 5000);
  }, [clearPolling, refreshProfile, user?.id]);

  const requestQRCode = async () => {
    if (!user) return;
    setConnectingWhatsApp(true);
    setQrCode(null);

    try {
      const { data: settings, error } = await supabase
        .from('api_settings')
        .select('whatsapp_backend_url, whatsapp_api_key')
        .eq('id', 1)
        .single();

      if (error || !settings?.whatsapp_backend_url) {
        throw new Error('WhatsApp backend not configured');
      }

      const response = await fetch(`${settings.whatsapp_backend_url}/api/session/start`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': settings.whatsapp_api_key || '',
        },
        body: JSON.stringify({ agentId: user.id }),
      });

      const data = await response.json();
      if (!response.ok || !data?.qrCode) {
        throw new Error(data?.message || 'QR code was not returned');
      }

      const imageSrc = data.qrCode.startsWith('data:image')
        ? data.qrCode
        : `data:image/png;base64,${data.qrCode}`;

      setQrCode(imageSrc);
      pollWhatsAppStatus(settings.whatsapp_backend_url, settings.whatsapp_api_key || '');
    } catch (err: any) {
      setConnectingWhatsApp(false);
      toast.error(err.message || 'Failed to request QR code');
    }
  };

  const disconnectWhatsApp = async () => {
    if (!user) return;

    try {
      const { data: settings, error } = await supabase
        .from('api_settings')
        .select('whatsapp_backend_url, whatsapp_api_key')
        .eq('id', 1)
        .single();

      if (error || !settings?.whatsapp_backend_url) {
        throw new Error('WhatsApp backend not configured');
      }

      await fetch(`${settings.whatsapp_backend_url}/api/session/disconnect`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': settings.whatsapp_api_key || '',
        },
        body: JSON.stringify({ agentId: user.id }),
      });

      clearPolling();
      setQrCode(null);
      setConnectingWhatsApp(false);
      await supabase.from('profiles').update({ whatsapp_session_status: 'disconnected' }).eq('id', user.id);
      await refreshProfile();
      toast.success('WhatsApp disconnected');
    } catch (err: any) {
      toast.error(err.message || 'Failed to disconnect WhatsApp');
    }
  };

  const togglePause = async (pause: boolean) => {
    setTogglingPause(true);
    try {
      const { data: settings } = await supabase.from('api_settings').select('whatsapp_backend_url, whatsapp_api_key').eq('id', 1).single();
      if (settings?.whatsapp_backend_url) {
        const endpoint = pause ? '/api/session/pause' : '/api/session/resume';
        await fetch(`${settings.whatsapp_backend_url}${endpoint}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': settings.whatsapp_api_key || '' },
          body: JSON.stringify({ agentId: user?.id }),
        });
      }
      await supabase.from('profiles').update({ sending_paused: pause }).eq('id', user?.id);
      setSendingPaused(pause);
      toast.success(pause ? 'Sending paused' : 'Sending resumed');
    } catch {
      toast.error('Failed to update sending status');
    }
    setTogglingPause(false);
  };

  const handleUpload = async () => {
    if (!batchName || !csvFile || !selectedTemplate) {
      toast.error('Please fill all required fields');
      return;
    }

    setUploading(true);

    try {
      const text = await csvFile.text();
      const rows = parseCSV(text);
      if (rows.length === 0) { toast.error('CSV file is empty'); setUploading(false); return; }

      const template = templates.find(t => t.id === selectedTemplate);
      if (!template) { toast.error('Template not found'); setUploading(false); return; }

      const { data: settings } = await supabase.from('api_settings').select('gemini_api_key').eq('id', 1).single();

      const totalContacts = rows.length;
      const { data: batch, error: batchError } = await supabase.from('batches').insert({
        batch_name: batchName,
        uploaded_by: user!.id,
        total_contacts: totalContacts,
        pending_count: totalContacts,
      }).select().single();
      if (batchError) throw batchError;

      const assignedAgentId = user!.id;
      const agentName = profile?.first_name || '';
      const contactInserts: any[] = [];

      // Generate up to 5 template variations upfront to avoid Gemini rate limits
      const templateVariations: string[] = [template.body];
      const numVariations = Math.min(5, rows.length);
      if (settings?.gemini_api_key && rows.length > 0) {
        for (let v = 1; v < numVariations; v++) {
          try {
            await new Promise(r => setTimeout(r, 4500));
            const geminiRes = await fetch(
              `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${settings.gemini_api_key}`,
              {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  contents: [{ parts: [{ text: `You are making a very slight variation of a WhatsApp message template to avoid spam detection. Only change 1-2 words maximum — for example swap "Hi" with "Hello", "reaching out" with "getting in touch", etc. Do NOT change any placeholder variables like {{owner_name}} or {{building_name}}. Do NOT change the meaning or structure. Return only the message text.\n\nTemplate: ${template.body}` }] }],
                }),
              }
            );
            const geminiData = await geminiRes.json();
            const aiText = geminiData?.candidates?.[0]?.content?.parts?.[0]?.text;
            if (aiText) templateVariations.push(aiText.trim());
          } catch { /* use existing variations */ }
        }
      }

      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const templateVariant = templateVariations[i % templateVariations.length];
        const generated = substituteTemplate(templateVariant, row, agentName);

        contactInserts.push({
          uploaded_batch_id: batch.id,
          owner_name: row.owner_name,
          building_name: row.building_name || batchName,
          unit_number: row.unit_number || '',
          number_1: row.phone,
          number_2: '',
          assigned_agent: assignedAgentId,
          generated_message: generated,
        });
      }

      const { error: insertError } = await supabase.from('owner_contacts').insert(contactInserts);
      if (insertError) throw insertError;

      toast.success(`${contactInserts.length} contacts added, assigned to you`);
      setBatchName('');
      setCsvFile(null);
      fetchData();
    } catch (err: any) {
      toast.error(err.message || 'Upload failed');
    }
    setUploading(false);
  };

  const viewContacts = async (batchId: string) => {
    setViewingBatch(batchId);
    const [contactsRes, profilesRes] = await Promise.all([
      supabase.from('owner_contacts').select('*').eq('uploaded_batch_id', batchId),
      supabase.from('profiles').select('id, first_name, last_name'),
    ]);
    const profileMap = Object.fromEntries((profilesRes.data || []).map(p => [p.id, p]));
    setContacts((contactsRes.data || []).map(c => ({
      ...c,
      agent_profile: profileMap[c.assigned_agent] || null,
    })));
  };

  const filteredContacts = contacts.filter(c => {
    if (filterStatus && filterStatus !== 'all' && c.message_status !== filterStatus) return false;
    if (filterAgent && filterAgent !== 'all' && c.assigned_agent !== filterAgent) return false;
    return true;
  });

  if (loading) return <div className="flex items-center justify-center py-20"><Loader2 className="animate-spin w-8 h-8 text-primary" /></div>;

  if (viewingBatch) {
    return (
      <div className="space-y-6">
        <Button variant="ghost" onClick={() => setViewingBatch(null)}><ArrowLeft className="w-4 h-4 mr-2" /> Back to Batches</Button>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between flex-wrap gap-2">
            <CardTitle>Contact Details</CardTitle>
            <div className="flex gap-2">
              {isAdmin && (
                <Select value={filterAgent} onValueChange={setFilterAgent}>
                  <SelectTrigger className="w-[150px]"><SelectValue placeholder="Filter agent" /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Agents</SelectItem>
                    {agents.map(a => <SelectItem key={a.id} value={a.id}>{a.first_name} {a.last_name}</SelectItem>)}
                  </SelectContent>
                </Select>
              )}
              <Select value={filterStatus} onValueChange={setFilterStatus}>
                <SelectTrigger className="w-[150px]"><SelectValue placeholder="Filter status" /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Statuses</SelectItem>
                  <SelectItem value="pending">Pending</SelectItem>
                  <SelectItem value="sent">Sent</SelectItem>
                  <SelectItem value="failed">Failed</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader><TableRow>
                <TableHead>Owner</TableHead><TableHead>Building</TableHead><TableHead>Number 1</TableHead><TableHead>Number 2</TableHead>
                {isAdmin && <TableHead>Agent</TableHead>}
                <TableHead>Message</TableHead><TableHead>Status</TableHead><TableHead>Sent At</TableHead>
              </TableRow></TableHeader>
              <TableBody>
                {filteredContacts.map(c => (
                  <TableRow key={c.id}>
                    <TableCell>{c.owner_name}</TableCell>
                    <TableCell>{c.building_name}</TableCell>
                    <TableCell>{c.number_1}</TableCell>
                    <TableCell>{c.number_2 || ''}</TableCell>
                    {isAdmin && <TableCell>{c.agent_profile ? `${c.agent_profile.first_name} ${c.agent_profile.last_name}` : ''}</TableCell>}
                    <TableCell className="max-w-[200px]">
                      <button onClick={() => setExpandedMsg(expandedMsg === c.id ? null : c.id)} className="text-left text-sm hover:text-primary">
                        {expandedMsg === c.id ? c.generated_message : (c.generated_message?.slice(0, 60) + (c.generated_message?.length > 60 ? '...' : ''))}
                      </button>
                    </TableCell>
                    <TableCell><Badge variant={c.message_status === 'sent' ? 'default' : 'secondary'}>{c.message_status}</Badge></TableCell>
                    <TableCell>{c.sent_at ? toUAETime(c.sent_at) : ''}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <Card>
        <CardHeader><CardTitle className="flex items-center gap-2"><Upload className="w-5 h-5" /> Upload New Batch</CardTitle></CardHeader>
        <CardContent className="space-y-4">
          <div>
            <label className="text-sm font-medium">Batch Name</label>
            <Input value={batchName} onChange={(e) => setBatchName(e.target.value)} placeholder="Enter batch name" className="mt-1" />
          </div>
          <div>
            <label className="text-sm font-medium">CSV File</label>
            <Input type="file" accept=".csv" onChange={(e) => setCsvFile(e.target.files?.[0] || null)} className="mt-1" />
            <p className="text-xs text-muted-foreground mt-1">Your CSV must contain at least one phone number column. Column headers should include words like mobile, phone, or number. Multiple number columns (mobile 1, mobile 2, mobile 3) are supported — each number creates a separate contact. All other columns are optional.</p>
          </div>
          <div>
            <label className="text-sm font-medium">Template</label>
            <Select value={selectedTemplate} onValueChange={setSelectedTemplate}>
              <SelectTrigger className="mt-1"><SelectValue placeholder="Select template" /></SelectTrigger>
              <SelectContent>
                {templates.map(t => <SelectItem key={t.id} value={t.id}>{t.template_name}{t.is_default ? ' (Default)' : ''}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
          <Button onClick={handleUpload} disabled={uploading} className="w-full sm:w-auto">
            {uploading ? <Loader2 className="animate-spin mr-2" /> : <Upload className="w-4 h-4 mr-2" />}
            Upload and Generate Messages
          </Button>
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle className="flex items-center gap-2"><QrCode className="w-5 h-5" /> WhatsApp Connection</CardTitle></CardHeader>
        <CardContent className="flex flex-col items-center gap-4">
          {isConnected ? (
            <>
              <Badge variant="default" className="bg-green-600 text-white text-base px-4 py-1">Connected</Badge>
              <div className="flex items-center gap-2">
                {sendingPaused ? (
                  <><span className="w-3 h-3 rounded-full bg-yellow-500" /><span className="text-sm font-medium text-yellow-600">Sending paused</span></>
                ) : (
                  <><span className="w-3 h-3 rounded-full bg-green-500 animate-pulse" /><span className="text-sm font-medium text-green-600">Sending active</span></>
                )}
              </div>
              <div className="flex gap-2">
                {sendingPaused ? (
                  <Button onClick={() => togglePause(false)} disabled={togglingPause} className="bg-green-600 hover:bg-green-700 text-white">
                    {togglingPause ? <Loader2 className="animate-spin mr-1 w-4 h-4" /> : <Play className="w-4 h-4 mr-1" />} Resume Sending
                  </Button>
                ) : (
                  <Button onClick={() => togglePause(true)} disabled={togglingPause} className="bg-yellow-500 hover:bg-yellow-600 text-white">
                    {togglingPause ? <Loader2 className="animate-spin mr-1 w-4 h-4" /> : <Pause className="w-4 h-4 mr-1" />} Pause Sending
                  </Button>
                )}
              </div>
              <p className="text-sm text-muted-foreground text-center">
                Your WhatsApp is active. Messages will send automatically between 9am and 7pm.
              </p>
              <Button variant="destructive" onClick={disconnectWhatsApp}>Disconnect</Button>
            </>
          ) : (
            <>
              {qrCode ? (
                <img src={qrCode} alt="WhatsApp QR Code" className="rounded-lg border" style={{ width: 256, height: 256 }} />
              ) : (
                <div className="flex items-center justify-center rounded-lg border-2 border-dashed bg-muted" style={{ width: 256, height: 256 }}>
                  <p className="px-4 text-center text-sm text-muted-foreground">Scan this code with your WhatsApp to connect</p>
                </div>
              )}
              <Button onClick={requestQRCode} disabled={connectingWhatsApp}>
                {connectingWhatsApp ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Request QR Code
              </Button>
              {connectingWhatsApp ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span>Connecting — keep this page open</span>
                </div>
              ) : null}
            </>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle>{isAdmin ? 'Active Batches' : 'Your Batches'}</CardTitle></CardHeader>
        <CardContent>
          {batches.length === 0 ? (
            <p className="text-muted-foreground text-sm">No batches uploaded yet</p>
          ) : (
            <Table>
              <TableHeader><TableRow>
                <TableHead>Batch Name</TableHead>{isAdmin && <TableHead>Uploaded By</TableHead>}<TableHead>Upload Date</TableHead><TableHead>Total</TableHead><TableHead>Sent</TableHead><TableHead>Pending</TableHead><TableHead>Progress</TableHead><TableHead></TableHead>
              </TableRow></TableHeader>
              <TableBody>
                {batches.map(b => (
                  <TableRow key={b.id}>
                    <TableCell className="font-medium">{b.batch_name}</TableCell>
                    {isAdmin && <TableCell>{b.uploader?.first_name} {b.uploader?.last_name}</TableCell>}
                    <TableCell>{toUAETime(b.upload_date)}</TableCell>
                    <TableCell>{b.total_contacts}</TableCell>
                    <TableCell>{b.sent_count}</TableCell>
                    <TableCell>{b.pending_count}</TableCell>
                    <TableCell className="w-32">
                      <Progress value={b.total_contacts > 0 ? (b.sent_count / b.total_contacts) * 100 : 0} />
                    </TableCell>
                    <TableCell>
                      <Button variant="ghost" size="sm" onClick={() => viewContacts(b.id)}><Eye className="w-4 h-4 mr-1" /> View</Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default UnitCollector;
