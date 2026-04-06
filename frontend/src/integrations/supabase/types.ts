export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  // Allows to automatically instantiate createClient with right options
  // instead of createClient<Database, { PostgrestVersion: 'XX' }>(URL, KEY)
  __InternalSupabase: {
    PostgrestVersion: "14.5"
  }
  public: {
    Tables: {
      activation_codes: {
        Row: {
          code: string
          created_at: string
          created_by: string | null
          id: string
          is_used: boolean
          used_at: string | null
          used_by: string | null
        }
        Insert: {
          code: string
          created_at?: string
          created_by?: string | null
          id?: string
          is_used?: boolean
          used_at?: string | null
          used_by?: string | null
        }
        Update: {
          code?: string
          created_at?: string
          created_by?: string | null
          id?: string
          is_used?: boolean
          used_at?: string | null
          used_by?: string | null
        }
        Relationships: []
      }
      api_settings: {
        Row: {
          gemini_api_key: string | null
          id: number
          updated_at: string
          whatsapp_api_key: string | null
          whatsapp_backend_url: string | null
        }
        Insert: {
          gemini_api_key?: string | null
          id?: number
          updated_at?: string
          whatsapp_api_key?: string | null
          whatsapp_backend_url?: string | null
        }
        Update: {
          gemini_api_key?: string | null
          id?: number
          updated_at?: string
          whatsapp_api_key?: string | null
          whatsapp_backend_url?: string | null
        }
        Relationships: []
      }
      batches: {
        Row: {
          batch_name: string
          created_at: string
          id: string
          pending_count: number
          sent_count: number
          total_contacts: number
          upload_date: string
          uploaded_by: string
        }
        Insert: {
          batch_name: string
          created_at?: string
          id?: string
          pending_count?: number
          sent_count?: number
          total_contacts?: number
          upload_date?: string
          uploaded_by: string
        }
        Update: {
          batch_name?: string
          created_at?: string
          id?: string
          pending_count?: number
          sent_count?: number
          total_contacts?: number
          upload_date?: string
          uploaded_by?: string
        }
        Relationships: []
      }
      message_templates: {
        Row: {
          body: string
          created_at: string
          created_by: string
          id: string
          is_default: boolean
          template_name: string
          updated_at: string
        }
        Insert: {
          body?: string
          created_at?: string
          created_by: string
          id?: string
          is_default?: boolean
          template_name: string
          updated_at?: string
        }
        Update: {
          body?: string
          created_at?: string
          created_by?: string
          id?: string
          is_default?: boolean
          template_name?: string
          updated_at?: string
        }
        Relationships: []
      }
      messages_log: {
        Row: {
          agent_id: string
          contact_id: string | null
          delivery_status: string
          id: string
          message_text: string
          number_used: string
          sent_at: string
        }
        Insert: {
          agent_id: string
          contact_id?: string | null
          delivery_status?: string
          id?: string
          message_text?: string
          number_used?: string
          sent_at?: string
        }
        Update: {
          agent_id?: string
          contact_id?: string | null
          delivery_status?: string
          id?: string
          message_text?: string
          number_used?: string
          sent_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "messages_log_contact_id_fkey"
            columns: ["contact_id"]
            isOneToOne: false
            referencedRelation: "owner_contacts"
            referencedColumns: ["id"]
          },
        ]
      }
      owner_contacts: {
        Row: {
          assigned_agent: string | null
          building_name: string
          created_at: string
          generated_message: string | null
          id: string
          message_status: string
          number_1: string
          number_2: string | null
          owner_name: string
          sent_at: string | null
          unit_number: string
          uploaded_batch_id: string | null
        }
        Insert: {
          assigned_agent?: string | null
          building_name?: string
          created_at?: string
          generated_message?: string | null
          id?: string
          message_status?: string
          number_1?: string
          number_2?: string | null
          owner_name: string
          sent_at?: string | null
          unit_number?: string
          uploaded_batch_id?: string | null
        }
        Update: {
          assigned_agent?: string | null
          building_name?: string
          created_at?: string
          generated_message?: string | null
          id?: string
          message_status?: string
          number_1?: string
          number_2?: string | null
          owner_name?: string
          sent_at?: string | null
          unit_number?: string
          uploaded_batch_id?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "owner_contacts_uploaded_batch_id_fkey"
            columns: ["uploaded_batch_id"]
            isOneToOne: false
            referencedRelation: "batches"
            referencedColumns: ["id"]
          },
        ]
      }
      profiles: {
        Row: {
          created_at: string
          email: string
          first_name: string
          id: string
          is_active: boolean
          last_login: string | null
          last_name: string
          role: Database["public"]["Enums"]["app_role"]
          sending_paused: boolean
          updated_at: string
          whatsapp_session_status: string
        }
        Insert: {
          created_at?: string
          email?: string
          first_name?: string
          id: string
          is_active?: boolean
          last_login?: string | null
          last_name?: string
          role?: Database["public"]["Enums"]["app_role"]
          sending_paused?: boolean
          updated_at?: string
          whatsapp_session_status?: string
        }
        Update: {
          created_at?: string
          email?: string
          first_name?: string
          id?: string
          is_active?: boolean
          last_login?: string | null
          last_name?: string
          role?: Database["public"]["Enums"]["app_role"]
          sending_paused?: boolean
          updated_at?: string
          whatsapp_session_status?: string
        }
        Relationships: []
      }
      user_roles: {
        Row: {
          id: string
          role: Database["public"]["Enums"]["app_role"]
          user_id: string
        }
        Insert: {
          id?: string
          role: Database["public"]["Enums"]["app_role"]
          user_id: string
        }
        Update: {
          id?: string
          role?: Database["public"]["Enums"]["app_role"]
          user_id?: string
        }
        Relationships: []
      }
    }
    Views: {
      [_ in never]: never
    }
    Functions: {
      has_role: {
        Args: {
          _role: Database["public"]["Enums"]["app_role"]
          _user_id: string
        }
        Returns: boolean
      }
    }
    Enums: {
      app_role: "super_admin" | "admin" | "agent"
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DatabaseWithoutInternals = Omit<Database, "__InternalSupabase">

type DefaultSchema = DatabaseWithoutInternals[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] &
        DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof DatabaseWithoutInternals },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof DatabaseWithoutInternals },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never,
> = PublicCompositeTypeNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {
      app_role: ["super_admin", "admin", "agent"],
    },
  },
} as const
