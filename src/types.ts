export interface Env {
  PIXEL_LOG?: KVNamespace;
  PIXEL_WEBHOOK_URL?: string;
  PIXEL_WEBHOOK_SECRET?: string;
}

export interface TokenPayload {
  tg: number;
  nonce: string;
}

export interface PixelEvent {
  tg_msg_id: number;
  layer: string;
  dimensions: string;
  classification: string;
  confidence: number;
  is_user_open: boolean;
  pixel_id: string;
  ip: string | null;
  user_agent: string | null;
  gmail_fetch_info: string | null;
  headers: Record<string, string>;
  path: string;
  received_at: string;
}
