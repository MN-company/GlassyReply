import { Env, PixelEvent, TokenPayload } from "./types";

const TRANSPARENT_PNG_2X1 = Uint8Array.from(
  atob("iVBORw0KGgoAAAANSUhEUgAAAAIAAAABCAYAAAD0In+KAAAAC0lEQVR4nGNggAIAAAkAAftSuKkAAAAASUVORK5CYII="),
  (char) => char.charCodeAt(0),
);

const PIXEL_PROBE_FONT = Uint8Array.from(
  atob(
    "d09GMgABAAAAAAM0AA4AAAAABjgAAALeAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG2wcNAZgP1NUQVReADwRDAqBEIEbCwgAATYCJAMMBCAFhGIHIBtIBcgEHnqd+v6X5ACzsYQkAymIUwF5CsyZerm95ZU3Bv/93CoaIt28FKjt7aGfYTaxiIgkUiZk8esLcUdrNBbxbDqIR2ZohQAisTIFddacRavI3NlW7yfTVa/7yPRvbwySiYoYeyEGfHp9kPCIMnxVJOGIhiQeI5FLqpCUFIVYhOh2JHbs/UTKBTbWF5V4qbvFEbQV7ZzmAJEXyMoDnM2Bg+zT58jQMvI3AvnZ+3OXLp3LNPIxm/hPJGfYOSkWxeUGmQnaAIYo46qHo/Ak2fteNbJlV6NblECLy90oyhGexu1+YUdSSiomFCRSlHcIs4H6VimqxZGyHC42pShj10+jxJbho5yMSj3GhLoiTD7mWVKBEnoReozAdmFVm+GhEHEZonyYNwFhQDHQ01FZ9WNRqOaD6A5iRVRbZEecpEUbsaKjNbKjrT2qbW3fs6j9T+i9H7nvofXkwJh9Y7LQBsYQeiR4LJrax+wbU35zU0lK4wTg0Rq13sPbLROmjRsHQxZz/7hx/R2mr0z/tfLs7cuW4a1xk76FZ4WXyiMfE8zBwzcHlym/M/89ibCFNwAdsVs68bMw//9oiFjwO/P/gghbOOZnRTnALvEXZDZHFnn6Lik0E7Y8SHFRGzFci/JqhGAZErEuQbSQLkksX10Kdu65VLK55dIopddlIptdzljGYA4RJURS7CojmnR9uTn6qLqfLwk70cB8GtmOHw87WYuOBxduGlmCQT2Bz8m1QnMXjbhV9Y3U0sAELFgwqEUniBODIME2oNBsQMdcf97FCgx2YPBqDGpYgY6LJvxsp541fLOeBjzw73wcmLFiYzQTyceOFTtjSLj8PfT557KUpcxlYn8HMzsoMnM2Qhv1a4D9v7JhI59VuNH73htJchkdNvCisxPLTKdpnWDepxvIp3zFGBeen6+a2IGZnRgEPmwbGLjwo68l04Cl5Z1ZkJ0jJg2rxe2k4upxhLyX+pAJAA==",
  ),
  (char) => char.charCodeAt(0),
);

type TrackRequest =
  | { kind: "image"; layer: string; dimensions: string; token: string }
  | { kind: "font"; layer: "font"; dimensions: "font"; token: string }
  | null;

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/healthz") {
      return json({ status: "ok" });
    }

    if (url.pathname === "/pixel") {
      const event = classifyLegacyRequest(request, url);
      ctx.waitUntil(reportEvent(env, event));
      return assetResponse("image/png", TRANSPARENT_PNG_2X1);
    }

    const trackRequest = parseTrackRequest(url);
    if (trackRequest) {
      const tokenPayload = await parseToken(trackRequest.token, env.PIXEL_WEBHOOK_SECRET || "");
      const event = classifyTrackRequest(request, trackRequest, tokenPayload);
      ctx.waitUntil(reportEvent(env, event));
      if (trackRequest.kind === "font") {
        return assetResponse("font/woff2", PIXEL_PROBE_FONT);
      }
      return assetResponse("image/png", TRANSPARENT_PNG_2X1);
    }

    return new Response("Not Found", { status: 404 });
  },
};

function parseTrackRequest(url: URL): TrackRequest {
  const parts = url.pathname.split("/").filter(Boolean);
  if (parts[0] !== "track") {
    return null;
  }
  if (parts[1] === "font" && parts[2]?.endsWith(".woff2")) {
    return {
      kind: "font",
      layer: "font",
      dimensions: "font",
      token: parts[2].replace(/\.woff2$/, ""),
    };
  }
  if (parts.length !== 4) {
    return null;
  }
  if (!["img", "bg", "dark"].includes(parts[1])) {
    return null;
  }
  if (!parts[3].endsWith(".png")) {
    return null;
  }
  return {
    kind: "image",
    layer: parts[1],
    dimensions: parts[2],
    token: parts[3].replace(/\.png$/, ""),
  };
}

async function parseToken(token: string, secret: string): Promise<TokenPayload> {
  if (!secret) {
    throw new Error("Missing PIXEL_WEBHOOK_SECRET");
  }
  const [payload, signature] = token.split(".");
  if (!payload || !signature) {
    throw new Error("Malformed token");
  }
  const expected = await signPayload(secret, payload);
  if (!timingSafeEqual(signature, expected)) {
    throw new Error("Invalid token signature");
  }
  const decoded = JSON.parse(new TextDecoder().decode(base64UrlToBytes(payload))) as TokenPayload;
  if (!decoded.tg || typeof decoded.tg !== "number") {
    throw new Error("Invalid token payload");
  }
  return decoded;
}

async function signPayload(secret: string, payload: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const digest = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload));
  return bytesToBase64Url(new Uint8Array(digest));
}

function classifyTrackRequest(
  request: Request,
  trackRequest: Exclude<TrackRequest, null>,
  tokenPayload: TokenPayload,
): PixelEvent {
  const base = baseEvent(request, tokenPayload.tg, trackRequest.layer, trackRequest.dimensions, trackRequest.token);
  const ua = (base.user_agent || "").toLowerCase();
  const gmailFetchInfo = (base.gmail_fetch_info || "").toLowerCase();
  const purpose = (
    request.headers.get("purpose") ||
    request.headers.get("sec-purpose") ||
    ""
  ).toLowerCase();
  const accept = (request.headers.get("accept") || "").toLowerCase();
  const secFetchDest = (request.headers.get("sec-fetch-dest") || "").toLowerCase();
  const hasBrowserFetchHeaders = Boolean(
    request.headers.get("sec-fetch-mode") ||
      request.headers.get("sec-fetch-site") ||
      request.headers.get("sec-fetch-dest"),
  );

  if (ua.includes("googleimageproxy") || gmailFetchInfo) {
    return { ...base, classification: "gmail_proxy", confidence: 0.98, is_user_open: false };
  }
  if (purpose.includes("prefetch")) {
    return { ...base, classification: "prefetch_proxy", confidence: 0.9, is_user_open: false };
  }
  if (
    trackRequest.layer === "font" ||
    secFetchDest === "font" ||
    accept.includes("font")
  ) {
    return { ...base, classification: "font_loader", confidence: 0.85, is_user_open: false };
  }
  if (
    hasBrowserFetchHeaders ||
    ua.includes("chrome") ||
    ua.includes("firefox") ||
    ua.includes("safari") ||
    ua.includes("edg/")
  ) {
    return { ...base, classification: "human_browser", confidence: 0.82, is_user_open: true };
  }
  return { ...base, classification: "unknown_proxy", confidence: 0.45, is_user_open: false };
}

function classifyLegacyRequest(request: Request, url: URL): PixelEvent {
  const tgMessageId = Number(url.searchParams.get("tg_msg_id") || 0);
  const base = baseEvent(request, tgMessageId, "legacy_img", "legacy", url.searchParams.get("id") || crypto.randomUUID());
  const ua = (base.user_agent || "").toLowerCase();
  const gmailFetchInfo = (base.gmail_fetch_info || "").toLowerCase();

  if (ua.includes("googleimageproxy") || gmailFetchInfo) {
    return { ...base, classification: "gmail_proxy", confidence: 0.98, is_user_open: false };
  }
  if (ua.includes("chrome") || ua.includes("firefox") || ua.includes("safari") || ua.includes("edg/")) {
    return { ...base, classification: "human_browser", confidence: 0.82, is_user_open: true };
  }
  return { ...base, classification: "unknown_proxy", confidence: 0.45, is_user_open: false };
}

function baseEvent(
  request: Request,
  tgMessageId: number,
  layer: string,
  dimensions: string,
  pixelId: string,
): PixelEvent {
  return {
    tg_msg_id: tgMessageId,
    layer,
    dimensions,
    classification: "unknown_proxy",
    confidence: 0.45,
    is_user_open: false,
    pixel_id: pixelId,
    ip: request.headers.get("cf-connecting-ip"),
    user_agent: request.headers.get("user-agent"),
    gmail_fetch_info: request.headers.get("x-gmail-fetch-info"),
    headers: interestingHeaders(request),
    path: new URL(request.url).pathname,
    received_at: new Date().toISOString(),
  };
}

async function reportEvent(env: Env, event: PixelEvent): Promise<void> {
  const body = JSON.stringify(event);
  if (env.PIXEL_LOG) {
    const key = `${event.tg_msg_id}:${event.layer}:${Date.now()}`;
    await env.PIXEL_LOG.put(key, body, { expirationTtl: 60 * 60 * 24 * 30 });
  }

  if (!env.PIXEL_WEBHOOK_URL || !env.PIXEL_WEBHOOK_SECRET) {
    console.warn("Skipping pixel webhook: missing PIXEL_WEBHOOK_URL or PIXEL_WEBHOOK_SECRET");
    return;
  }

  const response = await fetch(env.PIXEL_WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Pixel-Secret": env.PIXEL_WEBHOOK_SECRET,
    },
    body,
  });

  if (!response.ok) {
    const errorBody = await response.text();
    console.error(
      `Pixel webhook failed: ${response.status} ${response.statusText} ${errorBody}`,
    );
  }
}

function interestingHeaders(request: Request): Record<string, string> {
  const keys = [
    "accept",
    "accept-language",
    "cf-ipcountry",
    "cf-ray",
    "purpose",
    "sec-purpose",
    "sec-fetch-dest",
    "sec-fetch-mode",
    "sec-fetch-site",
    "user-agent",
    "x-gmail-fetch-info",
  ];
  const result: Record<string, string> = {};
  for (const key of keys) {
    const value = request.headers.get(key);
    if (value) {
      result[key] = value;
    }
  }
  return result;
}

function assetResponse(contentType: string, body: Uint8Array): Response {
  return new Response(body, {
    headers: {
      "content-type": contentType,
      "cache-control": "no-store, max-age=0",
      vary: "user-agent",
    },
  });
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function base64UrlToBytes(value: string): Uint8Array {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "=".repeat((4 - (normalized.length % 4 || 4)) % 4);
  return Uint8Array.from(atob(padded), (char) => char.charCodeAt(0));
}

function bytesToBase64Url(bytes: Uint8Array): string {
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function timingSafeEqual(left: string, right: string): boolean {
  if (left.length !== right.length) {
    return false;
  }
  let result = 0;
  for (let index = 0; index < left.length; index += 1) {
    result |= left.charCodeAt(index) ^ right.charCodeAt(index);
  }
  return result === 0;
}
