const HEADER = /^\s*(?:query|mutation|subscription)\s+(\w+)/;

export function operationName(document: string): string | undefined {
  return HEADER.exec(document)?.[1];
}
