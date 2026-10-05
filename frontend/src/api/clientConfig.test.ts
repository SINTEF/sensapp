import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import { client } from '../client/client.gen';
import { listMetrics } from '../client';
import { ApiError, configureApiClient, extractErrorMessage, unwrap } from './clientConfig';
import { useAuthStore } from '../stores/useAuthStore';

describe('ApiError and unwrap', () => {
  it('returns the data of a successful call', () => {
    expect(unwrap({ data: { ok: true } })).toEqual({ ok: true });
  });

  it('throws an ApiError with the status and the message of the server', () => {
    try {
      unwrap({ error: 'Invalid token: ExpiredSignature', response: { status: 401 } });
      expect.unreachable();
    } catch (error) {
      expect(error).toBeInstanceOf(ApiError);
      expect((error as ApiError).message).toBe('Invalid token: ExpiredSignature');
      expect((error as ApiError).status).toBe(401);
      expect((error as ApiError).isAuthError).toBe(true);
    }
  });

  it('knows which errors are about authentication', () => {
    expect(new ApiError('no', 401).isAuthError).toBe(true);
    expect(new ApiError('no', 403).isAuthError).toBe(true);
    expect(new ApiError('no', 400).isAuthError).toBe(false);
    expect(new ApiError('no', 500).isAuthError).toBe(false);
    expect(new ApiError('no').isAuthError).toBe(false);
  });
});

describe('the configured API client', () => {
  let authorization: string | null | undefined;
  let status = 200;

  beforeAll(() => {
    configureApiClient();
    client.setConfig({
      baseUrl: 'http://example.test',
      fetch: async (input) => {
        authorization = (input as Request).headers.get('Authorization');
        return status === 200
          ? new Response('{}', { headers: { 'Content-Type': 'application/json' } })
          : new Response('Invalid token: ExpiredSignature', { status });
      },
    });
  });

  beforeEach(() => {
    authorization = undefined;
    status = 200;
    useAuthStore.setState({ token: null });
  });

  it('sends no Authorization header without a token', async () => {
    await listMetrics();
    expect(authorization).toBeNull();
  });

  it('sends the token as a Bearer token', async () => {
    useAuthStore.getState().signIn('the.jwt.token');
    await listMetrics();
    expect(authorization).toBe('Bearer the.jwt.token');
  });

  it('turns a refused request into an authentication error', async () => {
    status = 401;
    const result = await listMetrics();
    expect(() => unwrap(result)).toThrow(
      expect.objectContaining({ status: 401, message: 'Invalid token: ExpiredSignature' }),
    );
  });
});

describe('extractErrorMessage', () => {
  it('extracts message from Error objects', () => {
    expect(extractErrorMessage(new Error('something broke'))).toBe('something broke');
  });

  it('returns string errors as-is', () => {
    expect(extractErrorMessage('connection refused')).toBe('connection refused');
  });

  it('extracts from SensApp AppError union types', () => {
    expect(extractErrorMessage({ BadRequest: 'Invalid query' })).toBe('Invalid query');
    expect(extractErrorMessage({ NotFound: 'Series not found' })).toBe('Series not found');
    expect(extractErrorMessage({ InternalServerError: 'DB down' })).toBe('DB down');
    expect(extractErrorMessage({ Storage: 'Connection lost' })).toBe('Connection lost');
  });

  it('extracts from objects with message property', () => {
    expect(extractErrorMessage({ message: 'fetch failed' })).toBe('fetch failed');
  });

  it('falls back to status code when available', () => {
    expect(extractErrorMessage({ status: 503 })).toBe('Request failed with status 503');
  });

  it('extracts from nested body objects', () => {
    expect(extractErrorMessage({ body: { BadRequest: 'Invalid input' } })).toBe('Invalid input');
  });

  it('returns default message for unknown types', () => {
    expect(extractErrorMessage(42)).toBe('An unexpected error occurred');
    expect(extractErrorMessage(null)).toBe('An unexpected error occurred');
    expect(extractErrorMessage(undefined)).toBe('An unexpected error occurred');
  });
});
