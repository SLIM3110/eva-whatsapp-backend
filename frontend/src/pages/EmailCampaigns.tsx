import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Mail } from 'lucide-react';

const EmailCampaigns = () => {
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2"><Mail className="w-5 h-5" /> Email Campaigns</CardTitle>
          <Badge variant="secondary">Coming Soon</Badge>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground">
            Email Campaigns will allow you to create and manage email sequences for property owners,
            track open and click rates, and integrate with the overall lead management pipeline.
          </p>
        </CardContent>
      </Card>
    </div>
  );
};

export default EmailCampaigns;
