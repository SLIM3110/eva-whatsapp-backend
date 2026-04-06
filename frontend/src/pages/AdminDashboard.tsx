import { useEffect, useState, useCallback } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { useAuth } from '@/contexts/AuthContext';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { getTodayStartUTC, toUAETime } from '@/lib/uaeTime';
import { toast } from 'sonner';
import { MessageSquare, Users, Wifi, AlertTriangle, Copy, Loader2, UserPlus, Trash2, Plus, QrCode, WifiOff, Pause, Play } from 'lucide-react';

const AdminDashboard = () => {
  const { user, profile, refreshProfile } = useAuth();
  const [stats, setStats] = useState({ totalSent: 0, activeAgents: 0 });
  const [agents, setAgents] = useState<any[]>([]);
  const [failedMessages, setFailedMessages] = useState<any[]>([]);
  const [disconnectedAgents, setDisconnectedAgents] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [generatedCode, setGeneratedCode] = useState<string | null>(null);
  const [generating, setGenerating] = useState(false);
  const [blacklist, setBlacklist] = useState<string[]>([]);
  const [newBlacklistNumber, setNewBlacklistNumber] = useState('');
  const [qrCode, setQrCode] = useState<string | null>(null);
  const [connectingWA, setConnectingWA] = useState(false);
  const [sendingPaused, setSendingPaused] = useState(false);
  const [togglingPause, setTogglingPause] = useState(false);

  const fetchData = useCallback(async () => {
    const todayStart = getTodayStartUTC();
    const now = new Date();
    const uaeNow = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Dubai' }));
    const monthStart = new Date(uaeNow.getFullYear(), uaeNow.getMonth(), 1);
    const monthStartUTC = new Date(monthStart.getTime() - 4 * 60 * 60 * 1000).toISOString();

    const [sentRes, agentsRes, profileRes] = await Promise.all([
      supabase.from('messages_log').select('id', { count: 'exact' }).gte('sent_at', todayStart),
      supabase.from('profiles').select('*').eq('role', 'agent'),
      user ? supabase.from('profiles').select('sending_paused').eq('id', user.id).single() : Promise.resolve({ data: null }),
    ]);
    setSendingPaused(profileRes.data?.sending_paused ?? false);

    const agentData = agentsRes.data || [];
    setStats({
      totalSent: sentRes.count || 0,
      activeAgents: agentData.filter(a => a.is_active).length,
    });

    const agentIds = agentData.map(a => a.id);

    if (agentIds.length > 0) {
      const { data: agentMessages } = await supabase
        .from('messages_log')
        .select('agent_id, sent_at, delivery_status, number_used, contact_id')
        .in('agent_id', agentIds)
        .gte('sent_at', todayStart);

      const agentMsgCounts: Record<string, number> = {};
      const agentLastActive: Record<string, string> = {};
      const failed: any[] = [];

      (agentMessages || []).forEach(m => {
        agentMsgCounts[m.agent_id] = (agentMsgCounts[m.agent_id] || 0) + 1;
        if (!agentLastActive[m.agent_id] || m.sent_at > agentLastActive[m.agent_id]) {
          agentLastActive[m.agent_id] = m.sent_at;
        }
        if (m.delivery_status === 'failed') {
          const agent = agentData.find(a => a.id === m.agent_id);
          failed.push({ ...m, agent_name: agent ? `${agent.first_name} ${agent.last_name}` : 'Unknown' });
        }
      });

      setAgents(agentData.map(a => ({
        ...a,
        sentToday: agentMsgCounts[a.id] || 0,
        lastActive: agentLastActive[a.id] || null,
      })));

      setFailedMessages(failed.slice(0, 10));
      setDisconnectedAgents(agentData.filter(a => a.whatsapp_session_status !== 'connected'));
    } else {
      setAgents([]);
    }

    setLoading(false);
  }, [user]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const generateCode = async () => {
    setGenerating(true);
    const code = Math.floor(100000 + Math.random() * 900000).toString();
    const { error } = await supabase.from('activation_codes').insert({ code, created_by: user!.id });
    if (error) {
      const code2 = Math.floor(100000 + Math.random() * 900000).toString();
      const { error: e2 } = await supabase.from('activation_codes').insert({ code: code2, created_by: user!.id });
      if (e2) toast.error('Failed to generate code');
      else setGeneratedCode(code2);
    } else {
      setGeneratedCode(code);
    }
    setGenerating(false);
  };

  const addToBlacklist = () => {
    if (newBlacklistNumber.trim()) {
      setBlacklist(prev => [...prev, newBlacklistNumber.trim()]);
      setNewBlacklistNumber('');
      toast.success('Number added to blacklist');
    }
  };

  const removeFromBlacklist = (num: string) => {
    setBlacklist(prev => prev.filter(n => n !== num));
    toast.success('Number removed from blacklist');
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

  const requestQR = async () => {
    setConnectingWA(true);
    try {
      const { data: settings } = await supabase.from('api_settings').select('*').eq('id', 1).single();
      if (!settings?.whatsapp_backend_url) {
        toast.error('WhatsApp backend not configured');
        setConnectingWA(false);
        return;
      }
      const res = await fetch(`${settings.whatsapp_backend_url}/api/session/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': settings.whatsapp_api_key || '' },
        body: JSON.stringify({ agentId: user?.id }),
      });
      const data = await res.json();
      if (data.status === 'already_connected') {
        setConnectingWA(false);
        await supabase.from('profiles').update({ whatsapp_session_status: 'connected' }).eq('id', user?.id);
        await refreshProfile();
        toast.success('WhatsApp already connected!');
      } else if (data.qrCode) {
        setQrCode(data.qrCode);
        pollStatus(settings.whatsapp_backend_url, settings.whatsapp_api_key || '');
      }
    } catch {
      toast.error('Failed to connect to WhatsApp backend');
      setConnectingWA(false);
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
          setConnectingWA(false);
          await supabase.from('profiles').update({ whatsapp_session_status: 'connected' }).eq('id', user?.id);
          await refreshProfile();
          toast.success('WhatsApp connected!');
        } else if (data.qrCode) {
          setQrCode(data.qrCode);
        }
      } catch {
        clearInterval(interval);
        setConnectingWA(false);
      }
    }, 5000);
  };

  const disconnectWA = async () => {
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

  const isConnected = profile?.whatsapp_session_status === 'connected';

  if (loading) return <div className="flex items-center justify-center py-20"><Loader2 className="animate-spin w-8 h-8 text-primary" /></div>;

  return (
    <div className="space-y-8">
      {/* Summary Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Total Messages Today</CardTitle>
            <MessageSquare className="w-5 h-5 text-primary" />
          </CardHeader>
          <CardContent><p className="text-3xl font-bold text-primary">{stats.totalSent}</p></CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Active Agents</CardTitle>
            <Users className="w-5 h-5 text-accent" />
          </CardHeader>
          <CardContent><p className="text-3xl font-bold text-accent">{stats.activeAgents}</p></CardContent>
        </Card>
      </div>

      {/* WhatsApp Connection */}
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
              <Button variant="destructive" onClick={disconnectWA}>Disconnect</Button>
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
              <Button onClick={requestQR} disabled={connectingWA}>
                {connectingWA ? <Loader2 className="animate-spin mr-2" /> : null}
                Request QR Code
              </Button>
            </>
          )}
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left — Unit Collector Summary */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Agent Overview</CardTitle>
          </CardHeader>
          <CardContent>
            {agents.length === 0 ? (
              <p className="text-muted-foreground text-sm">No agents registered</p>
            ) : (
              <div className="space-y-3">
                {agents.map(a => (
                  <div key={a.id} className="flex items-center justify-between p-3 rounded-lg border bg-card">
                    <div>
                      <p className="font-medium text-sm">{a.first_name} {a.last_name}</p>
                      <p className="text-xs text-muted-foreground">{a.sentToday}/50 sent • {a.lastActive ? toUAETime(a.lastActive) : 'Never active'}</p>
                    </div>
                    <div className="flex items-center gap-2">
                      {a.sending_paused && <Badge variant="secondary" className="text-xs">Paused</Badge>}
                      <span className={`w-3 h-3 rounded-full ${a.whatsapp_session_status === 'connected' ? 'bg-green-500' : 'bg-destructive'}`} />
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Centre — Salem Engine Summary */}
        <Card>
          <CardHeader><CardTitle className="text-base">Salem Engine</CardTitle></CardHeader>
          <CardContent>
            <p className="text-muted-foreground text-sm mb-4">Sequence stage breakdown</p>
            <div className="space-y-2">
              {['Day 1', 'Day 3', 'Day 7', 'Completed', 'Paused'].map(stage => (
                <div key={stage} className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">{stage}</span>
                  <Badge variant="secondary">0</Badge>
                </div>
              ))}
            </div>
            <div className="mt-4 pt-4 border-t">
              <p className="text-sm"><span className="text-muted-foreground">Total Leads:</span> <strong>0</strong></p>
              <p className="text-sm"><span className="text-muted-foreground">Reply Rate:</span> <strong>0%</strong></p>
            </div>
          </CardContent>
        </Card>

        {/* Right — System Health */}
        <Card>
          <CardHeader><CardTitle className="text-base">System Health</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            <div>
              <p className="text-sm font-medium text-muted-foreground mb-2">Failed Messages (24h)</p>
              {failedMessages.length === 0 ? (
                <p className="text-sm text-green-600">No failures</p>
              ) : (
                <div className="space-y-2">
                  {failedMessages.map(m => (
                    <div key={m.id} className="text-xs p-2 rounded border bg-destructive/5">
                      <p className="font-medium">{m.agent_name}</p>
                      <p className="text-muted-foreground">{m.number_used} • {toUAETime(m.sent_at)}</p>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div>
              <p className="text-sm font-medium text-muted-foreground mb-2">Disconnected Agents</p>
              {disconnectedAgents.length === 0 ? (
                <p className="text-sm text-green-600">All agents connected</p>
              ) : (
                <div className="space-y-2">
                  {disconnectedAgents.map(a => (
                    <div key={a.id} className="flex items-center justify-between text-xs p-2 rounded border bg-destructive/5">
                      <div className="flex items-center gap-2">
                        <AlertTriangle className="w-3 h-3 text-destructive" />
                        <span>{a.first_name} {a.last_name}</span>
                      </div>
                      <span className="text-muted-foreground">Needs reconnect</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Quick Actions */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base">Quick Actions</CardTitle>
            <Button onClick={generateCode} disabled={generating} size="sm">
              {generating ? <Loader2 className="animate-spin mr-2 w-4 h-4" /> : <UserPlus className="w-4 h-4 mr-2" />}
              Generate Activation Code
            </Button>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">Generate activation codes for new users and manage system settings from here.</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle className="text-base">Salem Blacklist</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            <div className="flex gap-2">
              <Input placeholder="Enter phone number" value={newBlacklistNumber} onChange={e => setNewBlacklistNumber(e.target.value)} />
              <Button size="sm" onClick={addToBlacklist}><Plus className="w-4 h-4" /></Button>
            </div>
            {blacklist.length === 0 ? (
              <p className="text-sm text-muted-foreground">No blacklisted numbers</p>
            ) : (
              <div className="space-y-1">
                {blacklist.map(num => (
                  <div key={num} className="flex items-center justify-between text-sm p-2 rounded border">
                    <span className="font-mono">{num}</span>
                    <Button variant="ghost" size="icon" onClick={() => removeFromBlacklist(num)}><Trash2 className="w-3 h-3 text-destructive" /></Button>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Generated Code Modal */}
      <Dialog open={!!generatedCode} onOpenChange={() => setGeneratedCode(null)}>
        <DialogContent>
          <DialogHeader><DialogTitle>Activation Code Generated</DialogTitle></DialogHeader>
          <div className="text-center space-y-4">
            <p className="text-4xl font-mono font-bold tracking-widest text-primary">{generatedCode}</p>
            <p className="text-sm text-muted-foreground">Share this code with the user to activate their account</p>
            <Button onClick={() => { navigator.clipboard.writeText(generatedCode!); toast.success('Copied!'); }} className="w-full">
              <Copy className="w-4 h-4 mr-2" /> Copy Code
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default AdminDashboard;
