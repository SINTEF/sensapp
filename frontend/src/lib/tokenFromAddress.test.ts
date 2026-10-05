import { beforeEach, describe, expect, it } from 'vitest';
import { useAuthStore } from '../stores/useAuthStore';
import { pickUpTokenFromAddress } from './tokenFromAddress';

function openAddress(address: string) {
  window.history.replaceState(null, '', address);
}

describe('pickUpTokenFromAddress', () => {
  beforeEach(() => {
    sessionStorage.clear();
    useAuthStore.setState({ token: null, authRequired: false, dialogOpen: false, message: null });
    openAddress('/');
  });

  it('signs in with the token of the fragment and takes it out of the address', () => {
    openAddress('/ui/?series=a#token=abc.def.ghi');
    pickUpTokenFromAddress();
    expect(useAuthStore.getState().token).toBe('abc.def.ghi');
    expect(window.location.hash).toBe('');
    // The rest of the address stays
    expect(window.location.pathname + window.location.search).toBe('/ui/?series=a');
  });

  it('keeps the other parts of the fragment', () => {
    openAddress('/ui/#token=abc&tab=1');
    pickUpTokenFromAddress();
    expect(useAuthStore.getState().token).toBe('abc');
    expect(window.location.hash).toBe('#tab=1');
  });

  it('does nothing without a token in the address', () => {
    openAddress('/ui/#section');
    pickUpTokenFromAddress();
    expect(useAuthStore.getState().token).toBeNull();
    expect(window.location.hash).toBe('#section');
  });

  it('forgets an empty token without signing in', () => {
    openAddress('/ui/#token=');
    pickUpTokenFromAddress();
    expect(useAuthStore.getState().token).toBeNull();
    expect(window.location.hash).toBe('');
  });
});
