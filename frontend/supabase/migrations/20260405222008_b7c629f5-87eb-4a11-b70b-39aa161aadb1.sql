
CREATE TYPE public.app_role AS ENUM ('super_admin', 'admin', 'agent');

CREATE TABLE public.profiles (
  id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  first_name TEXT NOT NULL DEFAULT '',
  last_name TEXT NOT NULL DEFAULT '',
  email TEXT NOT NULL DEFAULT '',
  role app_role NOT NULL DEFAULT 'agent',
  is_active BOOLEAN NOT NULL DEFAULT false,
  whatsapp_session_status TEXT NOT NULL DEFAULT 'disconnected',
  last_login TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

CREATE TABLE public.user_roles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE NOT NULL,
  role app_role NOT NULL,
  UNIQUE (user_id, role)
);

ALTER TABLE public.user_roles ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE FUNCTION public.has_role(_user_id UUID, _role app_role)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT EXISTS (
    SELECT 1 FROM public.user_roles WHERE user_id = _user_id AND role = _role
  )
$$;

CREATE POLICY "Users can view own profile" ON public.profiles FOR SELECT USING (auth.uid() = id);
CREATE POLICY "Admins can view all profiles" ON public.profiles FOR SELECT USING (
  public.has_role(auth.uid(), 'super_admin') OR public.has_role(auth.uid(), 'admin')
);
CREATE POLICY "Users can update own profile" ON public.profiles FOR UPDATE USING (auth.uid() = id);
CREATE POLICY "Super admins can update any profile" ON public.profiles FOR UPDATE USING (
  public.has_role(auth.uid(), 'super_admin')
);
CREATE POLICY "Allow insert for authenticated users" ON public.profiles FOR INSERT WITH CHECK (auth.uid() = id);

CREATE POLICY "Users can view own roles" ON public.user_roles FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Super admins can manage roles" ON public.user_roles FOR ALL USING (
  public.has_role(auth.uid(), 'super_admin')
);

CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  INSERT INTO public.profiles (id, first_name, last_name, email)
  VALUES (
    NEW.id,
    COALESCE(NEW.raw_user_meta_data->>'first_name', ''),
    COALESCE(NEW.raw_user_meta_data->>'last_name', ''),
    NEW.email
  );
  INSERT INTO public.user_roles (user_id, role) VALUES (NEW.id, 'agent');
  RETURN NEW;
END;
$$;

CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();

CREATE TABLE public.activation_codes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  code TEXT NOT NULL UNIQUE,
  created_by UUID REFERENCES auth.users(id),
  is_used BOOLEAN NOT NULL DEFAULT false,
  used_by UUID REFERENCES auth.users(id),
  used_at TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

ALTER TABLE public.activation_codes ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Super admins can manage activation codes" ON public.activation_codes FOR ALL USING (
  public.has_role(auth.uid(), 'super_admin')
);
CREATE POLICY "Authenticated users can view activation codes" ON public.activation_codes FOR SELECT USING (auth.uid() IS NOT NULL);
CREATE POLICY "Authenticated users can update activation codes" ON public.activation_codes FOR UPDATE USING (auth.uid() IS NOT NULL);

CREATE TABLE public.batches (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_name TEXT NOT NULL,
  uploaded_by UUID REFERENCES auth.users(id) NOT NULL,
  upload_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  total_contacts INTEGER NOT NULL DEFAULT 0,
  pending_count INTEGER NOT NULL DEFAULT 0,
  sent_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

ALTER TABLE public.batches ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Admins can view all batches" ON public.batches FOR SELECT USING (
  public.has_role(auth.uid(), 'super_admin') OR public.has_role(auth.uid(), 'admin')
);
CREATE POLICY "Admins can insert batches" ON public.batches FOR INSERT WITH CHECK (
  public.has_role(auth.uid(), 'super_admin') OR public.has_role(auth.uid(), 'admin')
);
CREATE POLICY "Admins can update batches" ON public.batches FOR UPDATE USING (
  public.has_role(auth.uid(), 'super_admin') OR public.has_role(auth.uid(), 'admin')
);

CREATE TABLE public.owner_contacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  uploaded_batch_id UUID REFERENCES public.batches(id) ON DELETE CASCADE,
  owner_name TEXT NOT NULL,
  building_name TEXT NOT NULL DEFAULT '',
  unit_number TEXT NOT NULL DEFAULT '',
  number_1 TEXT NOT NULL DEFAULT '',
  number_2 TEXT DEFAULT '',
  assigned_agent UUID REFERENCES auth.users(id),
  generated_message TEXT DEFAULT '',
  message_status TEXT NOT NULL DEFAULT 'pending',
  sent_at TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

ALTER TABLE public.owner_contacts ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Agents can view assigned contacts" ON public.owner_contacts FOR SELECT USING (
  auth.uid() = assigned_agent OR public.has_role(auth.uid(), 'super_admin') OR public.has_role(auth.uid(), 'admin')
);
CREATE POLICY "Admins can insert contacts" ON public.owner_contacts FOR INSERT WITH CHECK (
  public.has_role(auth.uid(), 'super_admin') OR public.has_role(auth.uid(), 'admin')
);
CREATE POLICY "Admins and agents can update contacts" ON public.owner_contacts FOR UPDATE USING (
  auth.uid() = assigned_agent OR public.has_role(auth.uid(), 'super_admin') OR public.has_role(auth.uid(), 'admin')
);

CREATE TABLE public.message_templates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  template_name TEXT NOT NULL,
  body TEXT NOT NULL DEFAULT '',
  created_by UUID REFERENCES auth.users(id) NOT NULL,
  is_default BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

ALTER TABLE public.message_templates ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can view all templates" ON public.message_templates FOR SELECT USING (auth.uid() IS NOT NULL);
CREATE POLICY "Users can create templates" ON public.message_templates FOR INSERT WITH CHECK (auth.uid() = created_by);
CREATE POLICY "Users can update own templates" ON public.message_templates FOR UPDATE USING (auth.uid() = created_by);
CREATE POLICY "Users can delete own templates" ON public.message_templates FOR DELETE USING (auth.uid() = created_by AND is_default = false);

CREATE TABLE public.messages_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  agent_id UUID REFERENCES auth.users(id) NOT NULL,
  contact_id UUID REFERENCES public.owner_contacts(id),
  message_text TEXT NOT NULL DEFAULT '',
  number_used TEXT NOT NULL DEFAULT '',
  delivery_status TEXT NOT NULL DEFAULT 'sent',
  sent_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

ALTER TABLE public.messages_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Agents can view own messages" ON public.messages_log FOR SELECT USING (
  auth.uid() = agent_id OR public.has_role(auth.uid(), 'super_admin') OR public.has_role(auth.uid(), 'admin')
);
CREATE POLICY "Agents can insert messages" ON public.messages_log FOR INSERT WITH CHECK (auth.uid() = agent_id);

CREATE TABLE public.api_settings (
  id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  whatsapp_backend_url TEXT DEFAULT '',
  whatsapp_api_key TEXT DEFAULT '',
  gemini_api_key TEXT DEFAULT '',
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

ALTER TABLE public.api_settings ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Super admins can manage api settings" ON public.api_settings FOR ALL USING (
  public.has_role(auth.uid(), 'super_admin')
);
CREATE POLICY "Authenticated users can read api settings" ON public.api_settings FOR SELECT USING (auth.uid() IS NOT NULL);

INSERT INTO public.api_settings (id) VALUES (1);

CREATE OR REPLACE FUNCTION public.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = public;

CREATE TRIGGER update_profiles_updated_at BEFORE UPDATE ON public.profiles FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();
CREATE TRIGGER update_templates_updated_at BEFORE UPDATE ON public.message_templates FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();
CREATE TRIGGER update_api_settings_updated_at BEFORE UPDATE ON public.api_settings FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();
