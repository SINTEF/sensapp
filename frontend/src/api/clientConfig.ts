import { client } from '../client/client.gen';
import { useAuthStore } from '../stores/useAuthStore';

export function configureApiClient() {
  const baseUrl = import.meta.env.VITE_SENSAPP_API_URL || '';
  client.setConfig({ baseUrl });
  client.interceptors.request.use((request) => {
    const { token } = useAuthStore.getState();
    if (token) {
      request.headers.set('Authorization', `Bearer ${token}`);
    }
    return request;
  });
}

/** An error answer of the API. 401 and 403 mean the token is missing, wrong or not enough. */
export class ApiError extends Error {
  readonly status?: number;

  constructor(message: string, status?: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }

  get isAuthError(): boolean {
    return this.status === 401 || this.status === 403;
  }
}

/**
 * The data of a generated client call, or an `ApiError` carrying the status and the message of
 * the server. The client returns errors instead of throwing them; React Query needs a throw.
 */
export function unwrap<T>(result: {
  data?: T;
  error?: unknown;
  response?: { status: number };
}): T {
  if (result.error) {
    throw new ApiError(extractErrorMessage(result.error), result.response?.status);
  }
  return result.data as T;
}

/**
 * Extract a human-readable error message from API errors.
 * hey-api/client-fetch returns the body of an error answer: parsed JSON, or the plain text
 * (which is what 401 and 403 are), or `{}` for an empty body.
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
