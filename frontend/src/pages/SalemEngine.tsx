import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Bot } from 'lucide-react';

const SalemEngine = () => {
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2"><Bot className="w-5 h-5" /> EVA Engine</CardTitle>
          <Badge variant="secondary">Coming Soon</Badge>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground">
            Salem Engine is an automated follow-up sequence system that manages off-plan lead nurturing
            with timed message sequences on Day 1, Day 3, and Day 7. It tracks reply rates, manages
            opt-outs, and automatically pauses sequences when leads respond.
          </p>
        </CardContent>
      </Card>
    </div>
  );
};

export default SalemEngine;
