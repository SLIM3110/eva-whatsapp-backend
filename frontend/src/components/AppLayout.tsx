import { useState } from 'react';
import { Link, Outlet, useLocation } from 'react-router-dom';
import { useAuth } from '@/contexts/AuthContext';
import evaLogo from '@/assets/eva-logo.jpg';
import { Button } from '@/components/ui/button';
import {
  LayoutDashboard,
  Building2,
  FileText,
  Settings,
  Users,
  LogOut,
  Menu,
  X,
  Bot,
  Mail,
  BarChart3,
  Loader2,
} from 'lucide-react';
import { cn } from '@/lib/utils';

const AppLayout = () => {
  const { profile, profileLoading, signOut } = useAuth();
  const location = useLocation();
  const [sidebarOpen, setSidebarOpen] = useState(false);

  const role = profile?.role;

  const navItems = [
    { path: '/', label: 'Dashboard', icon: LayoutDashboard, roles: ['super_admin', 'admin', 'agent'] },
    { path: '/unit-collector', label: 'Unit Collector', icon: Building2, roles: ['super_admin', 'admin', 'agent'] },
    { path: '/eva-engine', label: 'EVA Engine', icon: Bot, roles: ['super_admin'] },
    { path: '/email-campaigns', label: 'Email Campaigns', icon: Mail, roles: ['super_admin'] },
    { path: '/analytics', label: 'Analytics', icon: BarChart3, roles: ['super_admin', 'admin'] },
    { path: '/templates', label: 'Templates', icon: FileText, roles: ['super_admin', 'admin', 'agent'] },
    { path: '/settings', label: 'Settings', icon: Settings, roles: ['super_admin'] },
    { path: '/user-management', label: 'User Management', icon: Users, roles: ['super_admin'] },
  ];

  const filteredNav = role
    ? navItems.filter((item) => item.roles.includes(role))
    : navItems.filter((item) => item.roles.includes('agent'));

  return (
    <div className="min-h-screen flex bg-background">
      {sidebarOpen && (
        <div className="fixed inset-0 bg-black/50 z-40 lg:hidden" onClick={() => setSidebarOpen(false)} />
      )}

      <aside
        className={cn(
          'fixed lg:static inset-y-0 left-0 z-50 w-64 gradient-sidebar text-sidebar-foreground flex flex-col transition-transform duration-300',
          sidebarOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'
        )}
      >
        <div className="p-6 border-b border-sidebar-border flex items-center justify-center">
          <img src={evaLogo} alt="EVA Real Estate" className="h-14 w-auto max-w-full object-contain" />
        </div>

        <nav className="flex-1 p-4 space-y-1 overflow-y-auto">
          {profileLoading && (
            <div className="flex items-center justify-center py-4">
              <Loader2 className="animate-spin w-5 h-5 text-sidebar-foreground/50" />
            </div>
          )}
          {filteredNav.map((item) => (
            <Link
              key={item.path}
              to={item.path}
              onClick={() => setSidebarOpen(false)}
              className={cn(
                'flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-all duration-200',
                location.pathname === item.path
                  ? 'bg-sidebar-accent text-sidebar-primary'
                  : 'text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground'
              )}
            >
              <item.icon className="w-5 h-5" />
              {item.label}
            </Link>
          ))}
        </nav>

        <div className="p-4 border-t border-sidebar-border">
          <Button
            variant="ghost"
            onClick={signOut}
            className="w-full justify-start text-sidebar-foreground/70 hover:text-sidebar-foreground hover:bg-sidebar-accent/50"
          >
            <LogOut className="w-5 h-5 mr-3" />
            Sign Out
          </Button>
        </div>
      </aside>

      <div className="flex-1 flex flex-col min-h-screen">
        <header className="bg-card border-b px-4 lg:px-8 py-4 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Button
              variant="ghost"
              size="icon"
              className="lg:hidden"
              onClick={() => setSidebarOpen(!sidebarOpen)}
            >
              {sidebarOpen ? <X /> : <Menu />}
            </Button>
            <div>
              <h1 className="text-lg font-semibold text-primary">
                Hello {profile?.first_name || 'there'}
              </h1>
              <p className="text-sm text-muted-foreground">Let's make some deals happen!</p>
            </div>
          </div>
        </header>

        <main className="flex-1 p-4 lg:p-8 overflow-auto">
          <Outlet />
        </main>
      </div>
    </div>
  );
};

export default AppLayout;
