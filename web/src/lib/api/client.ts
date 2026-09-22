import type { Update } from "@/api/types.gen";

export class ApiError extends Error {
  readonly status: number;
  readonly code?: string;

  constructor(message: string, status: number, code?: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

export function apiErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    return error.code ? `${error.message} (${error.code})` : error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return fallback;
}

/** API calls use `/api` in dev and in the embedded UI. */
export function apiPath(path: string): string {
  return `/api${path}`;
}

type QueryValue = string | number | boolean | null | undefined;

function withQuery(path: string, query?: Record<string, QueryValue>): string {
  if (!query) return path;
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value == null || value === "") continue;
    params.set(key, String(value));
  }
  const text = params.toString();
  return text ? `${path}?${text}` : path;
}

/** Encode each path segment and keep `/`, so a catch-all route sees the real id. */
export function resourceId(id: string): string {
  return id
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

export const LOGIN_PATH = "/login";

const SIGN_IN_ATTEMPT_KEY = "klens.sign_in_attempt";
const SIGN_IN_RETRY_MS = 10_000;

/**
 * Where to send a signed-out browser: the identity provider. A second attempt within a few
 * seconds means the session did not stick, so stop on the login page instead of looping
 * through the IdP.
 */
export function signInHref(): string {
  const now = Date.now();
  let last = 0;
  try {
    last = Number(window.sessionStorage.getItem(SIGN_IN_ATTEMPT_KEY)) || 0;
    window.sessionStorage.setItem(SIGN_IN_ATTEMPT_KEY, String(now));
  } catch {
    // Without storage there is no loop guard; still sign in.
  }
  return now - last < SIGN_IN_RETRY_MS ? `${LOGIN_PATH}?error=auth` : apiPath("/auth/login");
}

function redirectToSignIn(): void {
  if (window.location.pathname === LOGIN_PATH) return;
  window.location.assign(signInHref());
}

function redirectIfUnauthorized(status: number): void {
  if (status !== 401) return;
  redirectToSignIn();
  throw new ApiError("Unauthorized", 401, "UNAUTHORIZED");
}

async function fail(response: Response): Promise<never> {
  redirectIfUnauthorized(response.status);
  let message = response.statusText || "Request failed";
  let code: string | undefined;
  try {
    const body = (await response.json()) as { error?: unknown; code?: unknown };
    if (typeof body.error === "string" && body.error) message = body.error;
    if (typeof body.code === "string") code = body.code;
  } catch {
    // A non-JSON body still becomes an ApiError from the status line.
  }
  throw new ApiError(message, response.status, code);
}

export async function get<T>(path: string, query?: Record<string, QueryValue>): Promise<T> {
  const response = await fetch(withQuery(apiPath(path), query), {
    credentials: "include",
    headers: { Accept: "application/json" },
  });
  if (!response.ok) return fail(response);
  return (await response.json()) as T;
}

/** A missing topic or group is 404. Pages treat that as an empty detail. */
export async function getOrNull<T>(
  path: string,
  query?: Record<string, QueryValue>,
): Promise<T | null> {
  try {
    return await get<T>(path, query);
  } catch (error) {
    if (
      error instanceof ApiError &&
      error.status === 404 &&
      (error.code === "UNKNOWN_TOPIC" || error.code === "UNKNOWN_GROUP")
    ) {
      return null;
    }
    throw error;
  }
}

const RETRY_ATTEMPTS = 8;

export function stream(
  path: string,
  query: Record<string, QueryValue> | undefined,
  onUpdate: (update: Update) => void,
): () => void {
  const controller = new AbortController();
  void pump(withQuery(apiPath(path), query), controller.signal, onUpdate);
  return () => controller.abort();
}

async function pump(url: string, signal: AbortSignal, onUpdate: (update: Update) => void) {
  let attempt = 0;
  while (!signal.aborted) {
    try {
      const response = await fetch(url, {
        credentials: "include",
        headers: { Accept: "text/event-stream" },
        signal,
      });
      const body = response.body;
      if (!response.ok || body == null) {
        await fail(response);
        return;
      }
      attempt = 0;
      await readEvents(body, signal, onUpdate);
    } catch (error) {
      if (signal.aborted) return;
      if (error instanceof ApiError && error.status === 401) return;
      console.error(error);
    }
    attempt += 1;
    if (attempt > RETRY_ATTEMPTS || signal.aborted) return;
    await wait(Math.min(1000 * 2 ** (attempt - 1), 10_000), signal);
  }
}

async function readEvents(
  body: ReadableStream<Uint8Array>,
  signal: AbortSignal,
  onUpdate: (update: Update) => void,
) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (!signal.aborted) {
      const { value, done } = await reader.read();
      if (done) return;
      buffer += decoder.decode(value, { stream: true });
      buffer = drain(buffer, onUpdate);
    }
  } finally {
    reader.releaseLock();
  }
}

function drain(buffer: string, onUpdate: (update: Update) => void): string {
  const chunks = buffer.split("\n\n");
  const rest = chunks.pop() ?? "";
  for (const chunk of chunks) {
    const update = parseEvent(chunk);
    if (update) onUpdate(update);
  }
  return rest;
}

function parseEvent(chunk: string): Update | null {
  const data = chunk
    .split("\n")
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trimStart())
    .join("\n");
  if (!data) return null;
  try {
    const parsed = JSON.parse(data) as Update | { code?: string };
    if (parsed && typeof parsed === "object" && "type" in parsed) return parsed;
    if (parsed && typeof parsed === "object" && parsed.code === "SESSION_EXPIRED") {
      redirectToSignIn();
    }
  } catch {
    // Keep-alive comments and truncated frames are not updates.
  }
  return null;
}

function wait(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}
