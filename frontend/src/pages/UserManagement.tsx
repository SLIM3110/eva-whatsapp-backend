import { useState, useEffect, useCallback } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { useAuth } from '@/contexts/AuthContext';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Switch } from '@/components/ui/switch';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { toast } from 'sonner';
import { toUAETime } from '@/lib/uaeTime';
import { Loader2, UserPlus, Copy } from 'lucide-react';

const UserManagement = () => {
  const { user } = useAuth();
  const [users, setUsers] = useState<any[]>([]);
  const [codes, setCodes] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [generatedCode, setGeneratedCode] = useState<string | null>(null);
  const [generating, setGenerating] = useState(false);

  const fetchData = useCallback(async () => {
    const [usersRes, codesRes] = await Promise.all([
      supabase.from('profiles').select('*').order('created_at', { ascending: false }),
      supabase.from('activation_codes').select('*').order('created_at', { ascending: false }),
    ]);
    const profileMap = Object.fromEntries((usersRes.data || []).map(p => [p.id, p]));
    setUsers(usersRes.data || []);
    setCodes((codesRes.data || []).map(c => ({
      ...c,
      used_by_profile: c.used_by ? profileMap[c.used_by] || null : null,
    })));
    setLoading(false);
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const changeRole = async (userId: string, newRole: string) => {
    const { error } = await supabase.from('profiles').update({ role: newRole as any }).eq('id', userId);
    if (error) toast.error('Failed to update role');
    else {
      // Also update user_roles
      await supabase.from('user_roles').delete().eq('user_id', userId);
      await supabase.from('user_roles').insert({ user_id: userId, role: newRole as any });
      toast.success('Role updated');
      fetchData();
    }
  };

  const toggleActive = async (userId: string, currentActive: boolean) => {
    const { error } = await supabase.from('profiles').update({ is_active: !currentActive }).eq('id', userId);
    if (error) toast.error('Failed to update');
    else { toast.success(currentActive ? 'User deactivated' : 'User activated'); fetchData(); }
  };

  const generateCode = async () => {
    setGenerating(true);
    const code = Math.floor(100000 + Math.random() * 900000).toString();
    const { error } = await supabase.from('activation_codes').insert({ code, created_by: user!.id });
    if (error) {
      if (error.code === '23505') {
        // duplicate, retry
        const code2 = Math.floor(100000 + Math.random() * 900000).toString();
        await supabase.from('activation_codes').insert({ code: code2, created_by: user!.id });
        setGeneratedCode(code2);
      } else {
        toast.error('Failed to generate code');
      }
    } else {
      setGeneratedCode(code);
    }
    setGenerating(false);
    fetchData();
  };

  const copyCode = () => {
    if (generatedCode) {
      navigator.clipboard.writeText(generatedCode);
      toast.success('Code copied to clipboard');
    }
  };

  const roleBadgeColor = (role: string) => {
    if (role === 'super_admin') return 'bg-accent text-accent-foreground';
    if (role === 'admin') return 'bg-primary text-primary-foreground';
    return 'bg-muted text-muted-foreground';
  };

  if (loading) return <div className="flex items-center justify-center py-20"><Loader2 className="animate-spin w-8 h-8 text-primary" /></div>;

  return (
    <div className="space-y-8">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>User Management</CardTitle>
          <Button onClick={generateCode} disabled={generating}>
            {generating ? <Loader2 className="animate-spin mr-2" /> : <UserPlus className="w-4 h-4 mr-2" />}
            Generate Activation Code
          </Button>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader><TableRow>
              <TableHead>Full Name</TableHead><TableHead>Email</TableHead><TableHead>Role</TableHead><TableHead>Active</TableHead><TableHead>Last Login</TableHead><TableHead>Actions</TableHead>
            </TableRow></TableHeader>
            <TableBody>
              {users.map(u => (
                <TableRow key={u.id}>
                  <TableCell className="font-medium">{u.first_name} {u.last_name}</TableCell>
                  <TableCell>{u.email}</TableCell>
                  <TableCell><Badge className={roleBadgeColor(u.role)}>{u.role.replace('_', ' ')}</Badge></TableCell>
                  <TableCell>
                    <Switch checked={u.is_active} onCheckedChange={() => toggleActive(u.id, u.is_active)} />
                  </TableCell>
                  <TableCell>{u.last_login ? toUAETime(u.last_login) : 'Never'}</TableCell>
                  <TableCell>
                    <Select value={u.role} onValueChange={(val) => changeRole(u.id, val)}>
                      <SelectTrigger className="w-[130px]"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="super_admin">Super Admin</SelectItem>
                        <SelectItem value="admin">Admin</SelectItem>
                        <SelectItem value="agent">Agent</SelectItem>
                      </SelectContent>
                    </Select>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Activation Codes */}
      <Card>
        <CardHeader><CardTitle>Activation Codes</CardTitle></CardHeader>
        <CardContent>
          <Table>
            <TableHeader><TableRow>
              <TableHead>Code</TableHead><TableHead>Created At</TableHead><TableHead>Status</TableHead><TableHead>Used By</TableHead><TableHead>Used At</TableHead>
            </TableRow></TableHeader>
            <TableBody>
              {codes.map(c => (
                <TableRow key={c.id}>
                  <TableCell className="font-mono font-bold">{c.code}</TableCell>
                  <TableCell>{toUAETime(c.created_at)}</TableCell>
                  <TableCell>
                    <Badge className={c.is_used ? 'bg-green-600 text-white' : 'bg-muted text-muted-foreground'}>
                      {c.is_used ? 'Used' : 'Unused'}
                    </Badge>
                  </TableCell>
                  <TableCell>{c.is_used && c.used_by_profile ? `${c.used_by_profile.first_name} ${c.used_by_profile.last_name}` : ''}</TableCell>
                  <TableCell>{c.used_at ? toUAETime(c.used_at) : ''}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Generated Code Modal */}
      <Dialog open={!!generatedCode} onOpenChange={() => setGeneratedCode(null)}>
        <DialogContent>
          <DialogHeader><DialogTitle>Activation Code Generated</DialogTitle></DialogHeader>
          <div className="text-center space-y-4">
            <p className="text-4xl font-mono font-bold tracking-widest text-primary">{generatedCode}</p>
            <p className="text-sm text-muted-foreground">Share this code with the user to activate their account</p>
            <Button onClick={copyCode} className="w-full"><Copy className="w-4 h-4 mr-2" /> Copy Code</Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default UserManagement;
