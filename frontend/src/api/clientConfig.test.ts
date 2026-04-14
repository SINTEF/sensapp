import { describe, it, expect } from 'vitest';
import { extractErrorMessage } from './clientConfig';

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
