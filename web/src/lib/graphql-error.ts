export class GraphQLError extends Error {
  readonly code?: string;

  constructor(message: string, code?: string) {
    super(message);
    this.name = "GraphQLError";
    this.code = code;
  }
}

export function graphqlErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof GraphQLError) {
    return error.code ? `${error.message} (${error.code})` : error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return fallback;
}
