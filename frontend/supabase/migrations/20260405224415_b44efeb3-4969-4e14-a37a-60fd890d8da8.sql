
-- Allow agents to insert batches (they upload their own)
CREATE POLICY "Agents can insert own batches"
ON public.batches FOR INSERT TO authenticated
WITH CHECK (auth.uid() = uploaded_by);

-- Allow agents to view their own batches
CREATE POLICY "Agents can view own batches"
ON public.batches FOR SELECT TO authenticated
USING (auth.uid() = uploaded_by);

-- Allow agents to update their own batches
CREATE POLICY "Agents can update own batches"
ON public.batches FOR UPDATE TO authenticated
USING (auth.uid() = uploaded_by);

-- Allow agents to insert contacts they are assigned to
CREATE POLICY "Agents can insert own contacts"
ON public.owner_contacts FOR INSERT TO authenticated
WITH CHECK (auth.uid() = assigned_agent);
