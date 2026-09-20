const HEADER = /^\s*(?:query|mutation|subscription)\s+(\w+)/;

export function operationName(document: { toString(): string }): string | undefined {
  return HEADER.exec(String(document))?.[1];
}
