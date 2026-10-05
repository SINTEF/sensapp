import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { usePrefersDark } from './usePrefersDark';

function mockScheme(initiallyDark: boolean) {
  let dark = initiallyDark;
  const listeners = new Set<() => void>();
  vi.stubGlobal('matchMedia', () => ({
    get matches() {
      return dark;
    },
    addEventListener: (_: string, listener: () => void) => listeners.add(listener),
    removeEventListener: (_: string, listener: () => void) => listeners.delete(listener),
  }));
  return {
    set(value: boolean) {
      dark = value;
      listeners.forEach((listener) => listener());
    },
    listeners,
  };
}

describe('usePrefersDark', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('reads the scheme of the OS', () => {
    mockScheme(true);
    expect(renderHook(() => usePrefersDark()).result.current).toBe(true);
    mockScheme(false);
    expect(renderHook(() => usePrefersDark()).result.current).toBe(false);
  });

  it('follows it when it changes, and stops listening on unmount', () => {
    const scheme = mockScheme(false);
    const { result, unmount } = renderHook(() => usePrefersDark());
    act(() => scheme.set(true));
    expect(result.current).toBe(true);
    act(() => scheme.set(false));
    expect(result.current).toBe(false);
    unmount();
    expect(scheme.listeners.size).toBe(0);
  });

  it('is light where the browser cannot tell', () => {
    vi.stubGlobal('matchMedia', undefined);
    expect(renderHook(() => usePrefersDark()).result.current).toBe(false);
  });
});
