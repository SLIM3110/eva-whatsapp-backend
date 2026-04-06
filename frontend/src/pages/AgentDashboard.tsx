import { useEffect, useState, useCallback } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { useAuth } from '@/contexts/AuthContext';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from '@/components/ui/dialog';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { toast } from 'sonner';
import { getTodayStartUTC, toUAETime } from '@/lib/uaeTime';
import {
  MessageSquare, Clock, Wifi, WifiOff, FileText, Plus, Edit2, Trash2, Loader2, QrCode, Eye, Pause, Play
} from 'lucide-react';

const AgentDashboard = () => {
  const { user, profile, refreshProfile } = useAuth();
  const [stats, setStats] = useState({ sentToday: 0, pending: 0, templates: 0 });
  const [pendingContacts, setPendingContacts] = useState<any[]>([]);
  const [sentToday, setSentToday] = useState<any[]>([]);
  const [templates, setTemplates] = useState<any[]>([]);
  const [qrCode, setQrCode] = useState<string | null>(null);
  const [connecting, setConnecting] = useState(false);
  const [expandedMsg, setExpandedMsg] = useState<string | null>(null);
  const [newTemplate, setNewTemplate] = useState({ name: '', body: '' });
  const [editTemplate, setEditTemplate] = useState<any>(null);
  const [templateDialogOpen, setTemplateDialogOpen] = useState(false);
  const [editDialogOpen, setEditDialogOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [sendingPaused, setSendingPaused] = useState(false);
  const [togglingPause, setTogglingPause] = useState(false);

  const fetchData = useCallback(async () => {
    if (!user) return;
    const todayStart = getTodayStartUTC();

    const [messagesRes, pendingRes, templatesRes, sentRes, contactsRes, profileRes] = await Promise.all([
      supabase.from('messages_log').select('id', { count: 'exact' }).eq('agent_id', user.id).gte('sent_at', todayStart),
      supabase.from('owner_contacts').select('*').eq('assigned_agent', user.id).eq('message_status', 'pending'),
      supabase.from('message_templates').select('*').eq('created_by', user.id),
      supabase.from('messages_log').select('*').eq('agent_id', user.id).gte('sent_at', todayStart).order('sent_at', { ascending: false }),
      supabase.from('owner_contacts').select('id, owner_name'),
      supabase.from('profiles').select('sending_paused').eq('id', user.id).single(),
    ]);

    const contactMap = Object.fromEntries((contactsRes.data || []).map(c => [c.id, c]));

    setSendingPaused(profileRes.data?.sending_paused ?? false);
    setStats({
      sentToday: messagesRes.count || 0,
      pending: pendingRes.data?.length || 0,
      templates: templatesRes.data?.length || 0,
    });
    setPendingContacts(pendingRes.data || []);
    setSentToday((sentRes.data || []).map(m => ({ ...m, contact_name: contactMap[m.contact_id]?.owner_name || '' })));
    setTemplates(templatesRes.data || []);
    setLoading(false);
  }, [user]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const requestQR = async () => {
    setConnecting(true);
    try {
      const { data: settings } = await supabase.from('api_settings').select('*').eq('id', 1).single();
      if (!settings?.whatsapp_backend_url) {
        toast.error('WhatsApp backend not configured');
        setConnecting(false);
        return;
      }
      const res = await fetch(`${settings.whatsapp_backend_url}/api/session/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': settings.whatsapp_api_key || '' },
        body: JSON.stringify({ agentId: user?.id }),
      });
      const data = await res.json();
      if (data.status === 'already_connected') {
        setConnecting(false);
        await supabase.from('profiles').update({ whatsapp_session_status: 'connected' }).eq('id', user?.id);
        await refreshProfile();
        toast.success('WhatsApp already connected!');
      } else if (data.qrCode) {
        setQrCode(data.qrCode);
        pollStatus(settings.whatsapp_backend_url, settings.whatsapp_api_key || '');
      }
    } catch {
      toast.error('Failed to connect to WhatsApp backend');
      setConnecting(false);
    }
  };

  const pollStatus = (url: string, key: string) => {
    const interval = setInterval(async () => {
      try {
        const res = await fetch(`${url}/api/session/status?agentId=${user?.id}`, {
          headers: { 'x-api-key': key },
        });
        const data = await res.json();
        if (data.status === 'connected') {
          clearInterval(interval);
          setQrCode(null);
          setConnecting(false);
          await supabase.from('profiles').update({ whatsapp_session_status: 'connected' }).eq('id', user?.id);
          await refreshProfile();
          toast.success('WhatsApp connected!');
        } else if (data.qrCode) {
          setQrCode(data.qrCode);
        }
      } catch {
        clearInterval(interval);
        setConnecting(false);
      }
    }, 5000);
  };

  const disconnect = async () => {
    const { data: settings } = await supabase.from('api_settings').select('*').eq('id', 1).single();
    if (settings?.whatsapp_backend_url) {
      try {
        await fetch(`${settings.whatsapp_backend_url}/api/session/disconnect`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': settings.whatsapp_api_key || '' },
          body: JSON.stringify({ agentId: user?.id }),
        });
      } catch { /* ignore */ }
    }
    await supabase.from('profiles').update({ whatsapp_session_status: 'disconnected' }).eq('id', user?.id);
    await refreshProfile();
    toast.success('WhatsApp disconnected');
  };

  const togglePause = async (pause: boolean) => {
    setTogglingPause(true);
    try {
      const { data: settings } = await supabase.from('api_settings').select('*').eq('id', 1).single();
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

  const saveTemplate = async () => {
    if (!newTemplate.name || !newTemplate.body) return;
    const { error } = await supabase.from('message_templates').insert({
      template_name: newTemplate.name,
      body: newTemplate.body,
      created_by: user!.id,
    });
    if (error) { console.error('[saveTemplate] error:', error); toast.error('Failed to save template'); }
    else {
      toast.success('Template created');
      setNewTemplate({ name: '', body: '' });
      setTemplateDialogOpen(false);
      fetchData();
    }
  };

  const updateTemplate = async () => {
    if (!editTemplate) return;
    const { error } = await supabase.from('message_templates').update({
      template_name: editTemplate.template_name,
      body: editTemplate.body,
    }).eq('id', editTemplate.id);
    if (error) toast.error('Failed to update template');
    else {
      toast.success('Template updated');
      setEditDialogOpen(false);
      setEditTemplate(null);
      fetchData();
    }
  };

  const deleteTemplate = async (id: string, isDefault: boolean) => {
    if (isDefault) { toast.error('Cannot delete the EVA Default template'); return; }
    const { error } = await supabase.from('message_templates').delete().eq('id', id);
    if (error) toast.error('Failed to delete');
    else { toast.success('Template deleted'); fetchData(); }
  };

  const isConnected = profile?.whatsapp_session_status === 'connected';

  if (loading) return <div className="flex items-center justify-center py-20"><Loader2 className="animate-spin w-8 h-8 text-primary" /></div>;

  return (
    <div className="space-y-8">
      {/* Stats */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card><CardHeader className="flex flex-row items-center justify-between pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Messages Sent Today</CardTitle>
          <MessageSquare className="w-5 h-5 text-primary" />
        </CardHeader><CardContent><p className="text-3xl font-bold text-primary">{stats.sentToday} <span className="text-lg text-muted-foreground">of 50</span></p></CardContent></Card>

        <Card><CardHeader className="flex flex-row items-center justify-between pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Pending Contacts</CardTitle>
          <Clock className="w-5 h-5 text-accent" />
        </CardHeader><CardContent><p className="text-3xl font-bold text-accent">{stats.pending}</p></CardContent></Card>

        <Card><CardHeader className="flex flex-row items-center justify-between pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">WhatsApp Status</CardTitle>
          {isConnected ? <Wifi className="w-5 h-5 text-green-600" /> : <WifiOff className="w-5 h-5 text-destructive" />}
        </CardHeader><CardContent>
          <Badge variant={isConnected ? 'default' : 'destructive'} className={isConnected ? 'bg-green-600 text-white' : ''}>
            {isConnected ? 'Connected' : 'Disconnected'}
          </Badge>
          {isConnected && <Button variant="ghost" size="sm" className="ml-2 text-destructive" onClick={disconnect}>Disconnect</Button>}
        </CardContent></Card>

        <Card><CardHeader className="flex flex-row items-center justify-between pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Templates Created</CardTitle>
          <FileText className="w-5 h-5 text-primary" />
        </CardHeader><CardContent><p className="text-3xl font-bold text-primary">{stats.templates}</p></CardContent></Card>
      </div>

      {/* WhatsApp Connection Section */}
      <Card>
        <CardHeader><CardTitle className="flex items-center gap-2"><QrCode className="w-5 h-5" /> WhatsApp Connection</CardTitle></CardHeader>
        <CardContent className="flex flex-col items-center gap-4">
          {isConnected ? (
            <>
              <Badge className="bg-green-600 text-white text-lg px-6 py-2">Connected</Badge>
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
              <Button variant="destructive" onClick={disconnect}>Disconnect</Button>
            </>
          ) : qrCode ? (
            <>
              <img src={qrCode} alt="WhatsApp QR Code" className="border rounded-lg" style={{ width: 256, height: 256 }} />
              <div className="flex items-center gap-2 text-muted-foreground">
                <Loader2 className="animate-spin w-4 h-4" /> Connecting — keep this page open
              </div>
            </>
          ) : (
            <>
              <div className="bg-muted rounded-lg flex items-center justify-center border-2 border-dashed" style={{ width: 256, height: 256 }}>
                <p className="text-sm text-muted-foreground text-center px-4">Scan this code with your WhatsApp to connect</p>
              </div>
              <Button onClick={requestQR} disabled={connecting}>
                {connecting ? <Loader2 className="animate-spin mr-2" /> : null}
                Request QR Code
              </Button>
            </>
          )}
        </CardContent>
      </Card>

      {/* Today's Queue */}
      <Card>
        <CardHeader><CardTitle>Today's Queue</CardTitle></CardHeader>
        <CardContent>
          {pendingContacts.length === 0 ? (
            <p className="text-muted-foreground text-sm">No pending contacts</p>
          ) : (
            <Table>
              <TableHeader><TableRow>
                <TableHead>Owner Name</TableHead><TableHead>Building</TableHead><TableHead>Number 1</TableHead><TableHead>Number 2</TableHead><TableHead>Message</TableHead><TableHead>Status</TableHead>
              </TableRow></TableHeader>
              <TableBody>
                {pendingContacts.map((c) => (
                  <TableRow key={c.id}>
                    <TableCell>{c.owner_name}</TableCell>
                    <TableCell>{c.building_name}</TableCell>
                    <TableCell>{c.number_1}</TableCell>
                    <TableCell>{c.number_2 || ''}</TableCell>
                    <TableCell className="max-w-[200px]">
                      <button onClick={() => setExpandedMsg(expandedMsg === c.id ? null : c.id)} className="text-left text-sm hover:text-primary">
                        {expandedMsg === c.id ? c.generated_message : (c.generated_message?.slice(0, 60) + (c.generated_message?.length > 60 ? '...' : ''))}
                      </button>
                    </TableCell>
                    <TableCell><Badge variant="secondary">Pending</Badge></TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* Sent Today */}
      <Card>
        <CardHeader><CardTitle>Sent Today</CardTitle></CardHeader>
        <CardContent>
          {sentToday.length === 0 ? (
            <p className="text-muted-foreground text-sm">No messages sent today</p>
          ) : (
            <Table>
              <TableHeader><TableRow>
                <TableHead>Owner Name</TableHead><TableHead>Number Used</TableHead><TableHead>Message</TableHead><TableHead>Sent At</TableHead><TableHead>Status</TableHead>
              </TableRow></TableHeader>
              <TableBody>
                {sentToday.map((m) => (
                  <TableRow key={m.id}>
                    <TableCell>{m.contact_name || ''}</TableCell>
                    <TableCell>{m.number_used}</TableCell>
                    <TableCell className="max-w-[200px]">
                      <button onClick={() => setExpandedMsg(expandedMsg === m.id ? null : m.id)} className="text-left text-sm hover:text-primary">
                        {expandedMsg === m.id ? m.message_text : (m.message_text?.slice(0, 60) + '...')}
                      </button>
                    </TableCell>
                    <TableCell>{toUAETime(m.sent_at)}</TableCell>
                    <TableCell><Badge className="bg-green-600 text-white">{m.delivery_status}</Badge></TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* Templates */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Message Templates</CardTitle>
          <Dialog open={templateDialogOpen} onOpenChange={setTemplateDialogOpen}>
            <DialogTrigger asChild>
              <Button size="sm"><Plus className="w-4 h-4 mr-1" /> Create New Template</Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader><DialogTitle>Create Template</DialogTitle></DialogHeader>
              <div className="space-y-4">
                <Input placeholder="Template name" value={newTemplate.name} onChange={(e) => setNewTemplate({ ...newTemplate, name: e.target.value })} />
                <Textarea placeholder="Template body" rows={5} value={newTemplate.body} onChange={(e) => setNewTemplate({ ...newTemplate, body: e.target.value })} />
                <p className="text-xs text-muted-foreground">
                  Available variables: {'{{owner_name}}'}, {'{{agent_first_name}}'}, {'{{building_name}}'}, {'{{unit_number}}'}
                </p>
              </div>
              <DialogFooter><Button onClick={saveTemplate}>Save Template</Button></DialogFooter>
            </DialogContent>
          </Dialog>
        </CardHeader>
        <CardContent>
          {templates.length === 0 ? (
            <p className="text-muted-foreground text-sm">No templates yet</p>
          ) : (
            <div className="space-y-3">
              {templates.map((t) => (
                <div key={t.id} className="flex items-start justify-between p-4 rounded-lg border bg-card hover:bg-muted/50 transition-colors">
                  <div className="flex-1">
                    <p className="font-medium text-sm">{t.template_name} {t.is_default && <Badge variant="secondary" className="ml-2 text-xs">Default</Badge>}</p>
                    <p className="text-sm text-muted-foreground mt-1">{t.body.slice(0, 80)}{t.body.length > 80 ? '...' : ''}</p>
                  </div>
                  <div className="flex gap-1 ml-4">
                    <Button variant="ghost" size="icon" onClick={() => { setEditTemplate(t); setEditDialogOpen(true); }}><Edit2 className="w-4 h-4" /></Button>
                    {!t.is_default && <Button variant="ghost" size="icon" onClick={() => deleteTemplate(t.id, t.is_default)}><Trash2 className="w-4 h-4 text-destructive" /></Button>}
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Edit Template Dialog */}
      <Dialog open={editDialogOpen} onOpenChange={setEditDialogOpen}>
        <DialogContent>
          <DialogHeader><DialogTitle>Edit Template</DialogTitle></DialogHeader>
          {editTemplate && (
            <div className="space-y-4">
              <Input value={editTemplate.template_name} onChange={(e) => setEditTemplate({ ...editTemplate, template_name: e.target.value })} />
              <Textarea rows={5} value={editTemplate.body} onChange={(e) => setEditTemplate({ ...editTemplate, body: e.target.value })} />
              <p className="text-xs text-muted-foreground">
                Available variables: {'{{owner_name}}'}, {'{{agent_first_name}}'}, {'{{building_name}}'}, {'{{unit_number}}'}
              </p>
            </div>
          )}
          <DialogFooter><Button onClick={updateTemplate}>Update Template</Button></DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default AgentDashboard;
