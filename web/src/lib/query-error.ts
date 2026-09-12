export function queryErrorMessage(
  isError: boolean,
  error: unknown,
  fallback: string,
): string | undefined {
  if (!isError) {
    return undefined;
  }
  return error instanceof Error ? error.message : fallback;
}
