import { formatRelative } from "./format";

export function catalogHealthCaption(args: {
  updatedAt?: string | null;
  lastError?: string | null;
  now?: number;
}): string | undefined {
  const parts: string[] = [];
  if (args.updatedAt) {
    parts.push(`Updated ${formatRelative(args.updatedAt, args.now)}`);
  }
  if (args.lastError) {
    parts.push(args.lastError);
  }
  return parts.length > 0 ? parts.join(" · ") : undefined;
}
