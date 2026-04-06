import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes, Navigate } from "react-router-dom";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AuthProvider, useAuth } from "@/contexts/AuthContext";
import AppLayout from "@/components/AppLayout";
import Login from "@/pages/Login";
import Signup from "@/pages/Signup";
import Activate from "@/pages/Activate";
import Dashboard from "@/pages/Dashboard";
import UnitCollector from "@/pages/UnitCollector";
import Templates from "@/pages/Templates";
import SettingsPage from "@/pages/Settings";
import UserManagement from "@/pages/UserManagement";
import SalemEngine from "@/pages/SalemEngine";

import EmailCampaigns from "@/pages/EmailCampaigns";
import Analytics from "@/pages/Analytics";
import NotFound from "@/pages/NotFound";
import { Loader2 } from "lucide-react";

const queryClient = new QueryClient();

const ProtectedRoute = ({ children, roles }: { children: React.ReactNode; roles?: string[] }) => {
  const { session, profile, loading } = useAuth();

  if (loading) return <div className="min-h-screen flex items-center justify-center"><Loader2 className="animate-spin w-8 h-8 text-primary" /></div>;
  if (!session) return <Navigate to="/login" replace />;
  if (profile && !profile.is_active) return <Navigate to="/activate" replace />;
  if (roles && profile && !roles.includes(profile.role)) return <Navigate to="/" replace />;

  return <>{children}</>;
};

const AuthRoute = ({ children }: { children: React.ReactNode }) => {
  const { session, profile, loading } = useAuth();

  if (loading) return <div className="min-h-screen flex items-center justify-center"><Loader2 className="animate-spin w-8 h-8 text-primary" /></div>;
  if (session && profile?.is_active) return <Navigate to="/" replace />;
  if (session && profile && !profile.is_active) return <Navigate to="/activate" replace />;

  return <>{children}</>;
};

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <BrowserRouter>
        <AuthProvider>
          <Routes>
            <Route path="/login" element={<AuthRoute><Login /></AuthRoute>} />
            <Route path="/signup" element={<AuthRoute><Signup /></AuthRoute>} />
            <Route path="/activate" element={<Activate />} />
            <Route element={<ProtectedRoute><AppLayout /></ProtectedRoute>}>
              <Route path="/" element={<Dashboard />} />
              <Route path="/unit-collector" element={<UnitCollector />} />
              <Route path="/templates" element={<Templates />} />
              <Route path="/eva-engine" element={<ProtectedRoute roles={['super_admin']}><SalemEngine /></ProtectedRoute>} />
              
              <Route path="/email-campaigns" element={<ProtectedRoute roles={['super_admin']}><EmailCampaigns /></ProtectedRoute>} />
              <Route path="/analytics" element={<ProtectedRoute roles={['super_admin', 'admin']}><Analytics /></ProtectedRoute>} />
              <Route path="/settings" element={<ProtectedRoute roles={['super_admin']}><SettingsPage /></ProtectedRoute>} />
              <Route path="/user-management" element={<ProtectedRoute roles={['super_admin']}><UserManagement /></ProtectedRoute>} />
            </Route>
            <Route path="*" element={<NotFound />} />
          </Routes>
        </AuthProvider>
      </BrowserRouter>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
