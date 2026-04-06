import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { BarChart3 } from 'lucide-react';

const Analytics = () => {
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2"><BarChart3 className="w-5 h-5" /> Analytics</CardTitle>
          <Badge variant="secondary">Coming Soon</Badge>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground">
            Comprehensive analytics dashboard showing message delivery rates, response rates,
            agent performance metrics, conversion funnels, and campaign ROI across all channels.
          </p>
        </CardContent>
      </Card>
    </div>
  );
};

export default Analytics;
