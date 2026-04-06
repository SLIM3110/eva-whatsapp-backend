import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Phone } from 'lucide-react';

const VapiCalls = () => {
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2"><Phone className="w-5 h-5" /> Vapi Voice Calls</CardTitle>
          <Badge variant="secondary">Coming Soon</Badge>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground">
            Vapi integration will enable automated voice calls to leads, track call outcomes,
            and feed data back into the Salem Engine pipeline for intelligent follow-up.
          </p>
        </CardContent>
      </Card>
    </div>
  );
};

export default VapiCalls;
