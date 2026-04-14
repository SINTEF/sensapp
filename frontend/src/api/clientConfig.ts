import { client } from '../client/client.gen';

export function configureApiClient() {
  const baseUrl = import.meta.env.VITE_SENSAPP_API_URL || '';
  client.setConfig({ baseUrl });
}

/**
 * Extract a human-readable error message from API errors.
 * hey-api/client-fetch throws raw response data on throwOnError,
 * which may be an object, string, or Error.
 */
export function extractErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  if (error && typeof error === 'object') {
    // hey-api error responses or AppError union types
    const obj = error as Record<string, unknown>;
    for (const key of ['message', 'InternalServerError', 'BadRequest', 'NotFound', 'Storage', 'error', 'detail']) {
      if (typeof obj[key] === 'string') {
        return obj[key] as string;
      }
    }
    // Nested body from fetch errors
    if (obj['body'] && typeof obj['body'] === 'object') {
      return extractErrorMessage(obj['body']);
    }
    // Fallback: try status code
    if (typeof obj['status'] === 'number') {
      return `Request failed with status ${obj['status']}`;
    }
  }
  return 'An unexpected error occurred';
}
