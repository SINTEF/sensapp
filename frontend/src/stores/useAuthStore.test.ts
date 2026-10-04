import { beforeEach, describe, expect, it } from 'vitest';
import { fakeToken } from '../test/fakeToken';
import { describeToken, normalizeToken, useAuthStore } from './useAuthStore';

describe('useAuthStore', () => {
  beforeEach(() => {
    sessionStorage.clear();
    useAuthStore.setState({ token: null, authRequired: false, dialogOpen: false, message: null });
  });

  it('opens the dialog with the answer of the server when a request is refused', () => {
    useAuthStore.getState().reject('Missing or invalid Authorization header');
    const state = useAuthStore.getState();
    expect(state.dialogOpen).toBe(true);
    expect(state.authRequired).toBe(true);
    expect(state.message).toBe('Missing or invalid Authorization header');
  });

  it('signs in with a token, closes the dialog and forgets the complaint', () => {
    useAuthStore.getState().reject('Invalid token: ExpiredSignature');
    useAuthStore.getState().signIn('  Bearer abc.def.ghi  ');
    const state = useAuthStore.getState();
    expect(state.token).toBe('abc.def.ghi');
    expect(state.dialogOpen).toBe(false);
    expect(state.message).toBeNull();
  });

  it('signs out but remembers that the server wants a token', () => {
    useAuthStore.getState().reject('no');
    useAuthStore.getState().signIn('abc');
    useAuthStore.getState().signOut();
    expect(useAuthStore.getState().token).toBeNull();
    expect(useAuthStore.getState().authRequired).toBe(true);
  });

  it('keeps the token in sessionStorage only, and nothing else', () => {
    useAuthStore.getState().reject('no');
    useAuthStore.getState().signIn('abc.def.ghi');
    expect(sessionStorage.getItem('sensapp-auth')).toContain('abc.def.ghi');
    expect(localStorage.getItem('sensapp-auth')).toBeNull();
    const stored = JSON.parse(sessionStorage.getItem('sensapp-auth')!);
    expect(Object.keys(stored.state)).toEqual(['token']);
  });

  it('can be reopened and closed by hand', () => {
    useAuthStore.getState().openDialog();
    expect(useAuthStore.getState().dialogOpen).toBe(true);
    useAuthStore.getState().closeDialog();
    expect(useAuthStore.getState().dialogOpen).toBe(false);
  });
});

describe('normalizeToken', () => {
  it('strips what comes with a pasted token', () => {
    expect(normalizeToken('abc')).toBe('abc');
    expect(normalizeToken('Bearer abc\n')).toBe('abc');
    expect(normalizeToken('bearer   abc')).toBe('abc');
    expect(normalizeToken('"abc"')).toBe('abc');
    expect(normalizeToken('   ')).toBe('');
  });
});

describe('describeToken', () => {
  it('reads the claims of a token', () => {
    const info = describeToken(fakeToken({ sub: 'alice', scope: 'read write', exp: 4_102_444_800 }));
    expect(info).toEqual({
      subject: 'alice',
      scope: 'read write',
      expires: new Date(4_102_444_800_000),
    });
  });

  it('reads a subject that is not ASCII', () => {
    expect(describeToken(fakeToken({ sub: 'Zoë' }))?.subject).toBe('Zoë');
  });

  it('returns null for what is not a token', () => {
    expect(describeToken('')).toBeNull();
    expect(describeToken('no-dots')).toBeNull();
    expect(describeToken('a.%%%.c')).toBeNull();
    expect(describeToken('a.bm90LWpzb24.c')).toBeNull();
  });
});
