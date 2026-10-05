import { describe, expect, it } from 'vitest';
import { parseSensorNames, tokenScopes } from './credentials';

describe('parseSensorNames', () => {
  it('trims, drops blanks and repeats, and keeps the order', () => {
    expect(parseSensorNames(' temperature ,humidity,, temperature ,  ')).toEqual(['temperature', 'humidity']);
  });

  it('is empty for nothing', () => {
    expect(parseSensorNames('')).toEqual([]);
    expect(parseSensorNames(' , ')).toEqual([]);
  });

  it('keeps the names that hold spaces, as sensors do', () => {
    expect(parseSensorNames('cpu usage_idle, mem used')).toEqual(['cpu usage_idle', 'mem used']);
  });
});

describe('tokenScopes', () => {
  it('lists what the token claims', () => {
    expect(tokenScopes({ scope: 'read write delete' })).toEqual(['read', 'write', 'delete']);
    expect(tokenScopes({ scope: 'admin' })).toEqual(['admin']);
  });

  it('knows the default of a token without the claim, and of one that cannot be read', () => {
    expect(tokenScopes({})).toEqual(['read', 'write']);
    expect(tokenScopes(null)).toEqual(['read', 'write']);
  });
});
