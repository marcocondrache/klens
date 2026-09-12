import { graphqlErrorMessage } from "@/lib/graphql-error";

export function catalogLookupMessage(args: {
  isPending: boolean;
  isError: boolean;
  error: unknown;
  data: unknown;
  missing: string;
  failed: string;
}): string | undefined {
  if (args.isError) {
    return graphqlErrorMessage(args.error, args.failed);
  }
  if (!args.isPending && args.data == null) {
    return args.missing;
  }
  return undefined;
}
