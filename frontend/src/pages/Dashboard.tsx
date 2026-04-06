import { useAuth } from '@/contexts/AuthContext';
import AgentDashboard from './AgentDashboard';
import AdminDashboard from './AdminDashboard';

const Dashboard = () => {
  const { profile } = useAuth();
  if (profile?.role === 'agent') return <AgentDashboard />;
  return <AdminDashboard />;
};

export default Dashboard;
