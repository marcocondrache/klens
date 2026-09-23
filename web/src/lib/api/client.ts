import type { EventSourceMessage } from "eventsource-parser";
import { EventSourceParserStream } from "eventsource-parser/stream";

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

type QueryScalar = string | number | boolean;
type QueryValue = QueryScalar | readonly QueryScalar[] | null | undefined;

function withQuery(path: string, query?: Record<string, QueryValue>): string {
  if (!query) return path;
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value == null || value === "") continue;
    for (const item of Array.isArray(value) ? value : [value]) params.append(key, String(item));
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

export const SIGN_IN_PATH = apiPath("/auth/login");

function redirectToSignIn(): void {
  if (window.location.pathname === LOGIN_PATH) return;
  window.location.assign(SIGN_IN_PATH);
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
  void pump(path, query, controller.signal, onUpdate);
  return () => controller.abort();
}

async function pump(
  path: string,
  query: Record<string, QueryValue> | undefined,
  signal: AbortSignal,
  onUpdate: (update: Update) => void,
) {
  let attempt = 0;
  while (!signal.aborted) {
    try {
      for await (const message of events(path, signal, query)) {
        attempt = 0;
        if (message.event === "error") throw streamError(message.data);
        onUpdate(JSON.parse(message.data) as Update);
      }
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

export async function* events(
  path: string,
  signal: AbortSignal,
  query?: Record<string, QueryValue>,
): AsyncGenerator<EventSourceMessage> {
  const response = await fetch(withQuery(apiPath(path), query), {
    credentials: "include",
    headers: { Accept: "text/event-stream" },
    signal,
  });
  if (!response.ok || response.body == null) return fail(response);

  const reader = response.body
    .pipeThrough(new TextDecoderStream())
    .pipeThrough(new EventSourceParserStream())
    .getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) return;
      yield value;
    }
  } finally {
    reader.releaseLock();
  }
}

export function streamError(data: string): ApiError {
  const body = JSON.parse(data) as { error: string; code: string };
  if (body.code === "SESSION_EXPIRED") redirectToSignIn();
  return new ApiError(body.error, 0, body.code);
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
