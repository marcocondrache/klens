import { apiErrorMessage } from "@/lib/api/client";

export function catalogLookupMessage(args: {
  isPending: boolean;
  isError: boolean;
  error: unknown;
  data: unknown;
  missing: string;
  failed: string;
}): string | undefined {
  if (args.isError) {
    return apiErrorMessage(args.error, args.failed);
  }
  if (!args.isPending && args.data == null) {
    return args.missing;
  }
  return undefined;
}
