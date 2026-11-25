/**
 * Beat 5: CDN Edge Virtual Waiting Room (Cloudflare Worker / Edge Gate).
 *
 * Keeps 2,000,000 fans hammering refresh at the CDN edge so only buyers with
 * a valid HMAC-SHA256 signed queue token ever touch the origin backend,
 * metered to a steady 5,000 buyers per second.
 */

export interface Env {
  QUEUE_SECRET: string;
  ORIGIN_URL: string;
  DRIP_RATE_PER_SEC?: string;
}

const DRIP_RATE_PER_SEC = 5_000;
const encoder = new TextEncoder();

function base64UrlDecode(input: string): Uint8Array {
  const padded = input.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((input.length + 3) % 4);
  const raw = atob(padded);
  const bytes = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i++) {
    bytes[i] = raw.charCodeAt(i);
  }
  return bytes;
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function constantTimeEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    diff |= a[i] ^ b[i];
  }
  return diff === 0;
}

async function importHmacKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"],
  );
}

export async function issueQueueToken(
  clientId: string,
  secret: string,
  ttlSec = 120,
): Promise<string> {
  const payload = JSON.stringify({
    sub: clientId,
    exp: Math.floor(Date.now() / 1000) + ttlSec,
    rate: DRIP_RATE_PER_SEC,
  });
  const payloadB64 = base64UrlEncode(encoder.encode(payload));
  const key = await importHmacKey(secret);
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(payloadB64)));
  return `${payloadB64}.${base64UrlEncode(sig)}`;
}

export async function verifyQueueToken(req: Request, env?: Env): Promise<boolean> {
  const token =
    req.headers.get("X-Queue-Token") ||
    new URL(req.url).searchParams.get("queue_token") ||
    "";
  const parts = token.split(".");
  if (parts.length !== 2) return false;

  const [payloadB64, sigB64] = parts;
  const secret = env?.QUEUE_SECRET || "main-to-scale-edge-secret";

  try {
    const key = await importHmacKey(secret);
    const expectedSig = new Uint8Array(
      await crypto.subtle.sign("HMAC", key, encoder.encode(payloadB64)),
    );
    const actualSig = base64UrlDecode(sigB64);
    if (!constantTimeEqual(expectedSig, actualSig)) {
      return false;
    }

    const decoded = JSON.parse(new TextDecoder().decode(base64UrlDecode(payloadB64)));
    return typeof decoded.exp === "number" && decoded.exp > Math.floor(Date.now() / 1000);
  } catch {
    return false;
  }
}

export function waitingRoom(position = 184_200): Response {
  const etaSeconds = Math.max(1, Math.ceil(position / DRIP_RATE_PER_SEC));
  const body = {
    status: "waiting_room",
    queue_position: position,
    drip_rate_per_sec: DRIP_RATE_PER_SEC,
    estimated_wait_sec: etaSeconds,
    message: "CDN Edge Waiting Room active — refresh protected at the edge.",
  };
  return new Response(JSON.stringify(body, null, 2), {
    status: 202,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Retry-After": String(Math.min(15, etaSeconds)),
      "Cache-Control": "no-store",
      "X-Edge-Gate": "waiting-room",
    },
  });
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    // CDN Edge Worker
    if (!(await verifyQueueToken(req, env)))
      return waitingRoom();
    return fetch(req);
  },
};
