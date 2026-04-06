import { useState, useEffect, useCallback } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { useAuth } from '@/contexts/AuthContext';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Badge } from '@/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog';
import { toast } from 'sonner';
import { Plus, Pencil, Trash2, Star, Loader2 } from 'lucide-react';

const Templates = () => {
  const { user } = useAuth();
  const [templates, setTemplates] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [templateName, setTemplateName] = useState('');
  const [templateBody, setTemplateBody] = useState('');
  const [saving, setSaving] = useState(false);

  const fetchTemplates = useCallback(async () => {
    const { data } = await supabase.from('message_templates').select('*').order('created_at', { ascending: false });
    setTemplates(data || []);
    setLoading(false);
  }, []);

  useEffect(() => { fetchTemplates(); }, [fetchTemplates]);

  const openCreate = () => {
    setEditingId(null);
    setTemplateName('');
    setTemplateBody('');
    setDialogOpen(true);
  };

  const openEdit = (t: any) => {
    setEditingId(t.id);
    setTemplateName(t.template_name);
    setTemplateBody(t.body);
    setDialogOpen(true);
  };

  const handleSave = async () => {
    if (!templateName.trim() || !templateBody.trim()) {
      toast.error('Please fill in all fields');
      return;
    }
    setSaving(true);
    if (editingId) {
      const { error } = await supabase.from('message_templates').update({
        template_name: templateName,
        body: templateBody,
      }).eq('id', editingId);
      if (error) { console.error('[updateTemplate]', error); toast.error('Failed to update template'); }
      else toast.success('Template updated');
    } else {
      const { error } = await supabase.from('message_templates').insert({
        template_name: templateName,
        body: templateBody,
        created_by: user!.id,
      });
      if (error) { console.error('[createTemplate]', error); toast.error('Failed to create template'); }
      else toast.success('Template created');
    }
    setSaving(false);
    setDialogOpen(false);
    fetchTemplates();
  };

  const handleDelete = async (id: string) => {
    const { error } = await supabase.from('message_templates').delete().eq('id', id);
    if (error) toast.error('Failed to delete template');
    else { toast.success('Template deleted'); fetchTemplates(); }
  };

  const handleSetDefault = async (id: string) => {
    const { error: e1 } = await supabase.from('message_templates').update({ is_default: false }).eq('created_by', user!.id);
    if (e1) console.error('[setDefault unset]', e1);
    const { error: e2 } = await supabase.from('message_templates').update({ is_default: true }).eq('id', id);
    if (e2) console.error('[setDefault set]', e2);
    toast.success('Default template updated');
    fetchTemplates();
  };

  if (loading) return <div className="flex items-center justify-center py-20"><Loader2 className="animate-spin w-8 h-8 text-primary" /></div>;

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Message Templates</CardTitle>
          <Button onClick={openCreate}><Plus className="w-4 h-4 mr-2" /> New Template</Button>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground mb-4">
            Use placeholders: <code className="bg-muted px-1 rounded">{'{{owner_name}}'}</code>, <code className="bg-muted px-1 rounded">{'{{building_name}}'}</code>, <code className="bg-muted px-1 rounded">{'{{unit_number}}'}</code>, <code className="bg-muted px-1 rounded">{'{{agent_first_name}}'}</code>. Gemini will make very slight word variations (e.g. "Hi" → "Hello") to keep messages natural.
          </p>
          {templates.length === 0 ? (
            <p className="text-muted-foreground text-sm">No templates yet. Create one to get started.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Body</TableHead>
                  <TableHead>Default</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {templates.map(t => (
                  <TableRow key={t.id}>
                    <TableCell className="font-medium">{t.template_name}</TableCell>
                    <TableCell className="max-w-[300px] text-sm truncate">{t.body}</TableCell>
                    <TableCell>
                      {t.is_default ? (
                        <Badge><Star className="w-3 h-3 mr-1" /> Default</Badge>
                      ) : (
                        <Button variant="ghost" size="sm" onClick={() => handleSetDefault(t.id)}>Set Default</Button>
                      )}
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-1">
                        <Button variant="ghost" size="icon" onClick={() => openEdit(t)}><Pencil className="w-4 h-4" /></Button>
                        {!t.is_default && (
                          <Button variant="ghost" size="icon" onClick={() => handleDelete(t.id)}><Trash2 className="w-4 h-4 text-destructive" /></Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{editingId ? 'Edit Template' : 'Create Template'}</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <label className="text-sm font-medium">Template Name</label>
              <Input value={templateName} onChange={(e) => setTemplateName(e.target.value)} placeholder="e.g. Standard Introduction" className="mt-1" />
            </div>
            <div>
              <label className="text-sm font-medium">Message Body</label>
              <Textarea value={templateBody} onChange={(e) => setTemplateBody(e.target.value)} placeholder="Hi {{owner_name}}, I'm {{agent_first_name}} from EVA..." rows={6} className="mt-1" />
            </div>
            <Button onClick={handleSave} disabled={saving} className="w-full">
              {saving ? <Loader2 className="animate-spin mr-2" /> : null}
              {editingId ? 'Update Template' : 'Create Template'}
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default Templates;
