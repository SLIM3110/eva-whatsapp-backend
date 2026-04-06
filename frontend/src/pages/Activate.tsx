import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { supabase } from '@/integrations/supabase/client';
import { useAuth } from '@/contexts/AuthContext';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { InputOTP, InputOTPGroup, InputOTPSlot } from '@/components/ui/input-otp';
import { toast } from 'sonner';
import { Loader2, ShieldCheck } from 'lucide-react';

const Activate = () => {
  const [code, setCode] = useState('');
  const [loading, setLoading] = useState(false);
  const { user, refreshProfile } = useAuth();
  const navigate = useNavigate();

  const handleActivate = async () => {
    if (code.length !== 6) return;
    setLoading(true);

    const { data: codeData, error: fetchError } = await supabase
      .from('activation_codes')
      .select('*')
      .eq('code', code)
      .eq('is_used', false)
      .single();

    if (fetchError || !codeData) {
      toast.error('Invalid or already used activation code');
      setLoading(false);
      return;
    }

    const { error: updateCodeError } = await supabase
      .from('activation_codes')
      .update({ is_used: true, used_by: user?.id, used_at: new Date().toISOString() })
      .eq('id', codeData.id);

    if (updateCodeError) {
      toast.error('Failed to activate. Please try again.');
      setLoading(false);
      return;
    }

    const { error: updateProfileError } = await supabase
      .from('profiles')
      .update({ is_active: true })
      .eq('id', user?.id);

    if (updateProfileError) {
      toast.error('Failed to activate profile. Please try again.');
      setLoading(false);
      return;
    }

    await refreshProfile();
    toast.success('Account activated successfully!');
    navigate('/');
    setLoading(false);
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-primary/10 via-background to-background p-4">
      <Card className="w-full max-w-md shadow-xl border-0">
        <CardHeader className="text-center space-y-4">
          <div className="mx-auto w-16 h-16 bg-primary/10 rounded-full flex items-center justify-center">
            <ShieldCheck className="w-8 h-8 text-primary" />
          </div>
          <CardTitle className="text-2xl font-bold text-primary">Account Activation</CardTitle>
          <CardDescription className="text-base">
            Your account is pending activation. Please enter your 6 digit activation code provided by your administrator.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col items-center gap-6">
          <InputOTP maxLength={6} value={code} onChange={(value) => setCode(value)}>
            <InputOTPGroup>
              <InputOTPSlot index={0} />
              <InputOTPSlot index={1} />
              <InputOTPSlot index={2} />
              <InputOTPSlot index={3} />
              <InputOTPSlot index={4} />
              <InputOTPSlot index={5} />
            </InputOTPGroup>
          </InputOTP>
          <Button onClick={handleActivate} className="w-full" disabled={loading || code.length !== 6}>
            {loading ? <Loader2 className="animate-spin mr-2" /> : null}
            Activate Account
          </Button>
        </CardContent>
      </Card>
    </div>
  );
};

export default Activate;
