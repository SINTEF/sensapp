import { renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { REFRESH_MS, useLiveRange } from './useLiveRange';
import { useSelectionStore } from '../stores/useSelectionStore';

describe('useLiveRange', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-10-05T10:00:00Z'));
    useSelectionStore.getState().setRelativeRange('1h');
  });
  afterEach(() => vi.useRealTimers());

  it('moves the end of a preset to now every minute', () => {
    renderHook(() => useLiveRange());
    expect(useSelectionStore.getState().timeRange.end).toBe('2026-10-05T10:00:00.000Z');

    vi.advanceTimersByTime(REFRESH_MS);
    const { timeRange, relativeRange } = useSelectionStore.getState();
    expect(timeRange).toEqual({ start: '2026-10-05T09:01:00.000Z', end: '2026-10-05T10:01:00.000Z' });
    expect(relativeRange).toBe('1h');
  });

  it('moves it as soon as the tab is shown again', () => {
    renderHook(() => useLiveRange());
    vi.setSystemTime(new Date('2026-10-05T12:00:00Z'));
    document.dispatchEvent(new Event('visibilitychange'));
    expect(useSelectionStore.getState().timeRange.end).toBe('2026-10-05T12:00:00.000Z');
  });

  it('leaves typed dates alone', () => {
    const typed = { start: '2026-10-01T00:00:00.000Z', end: '2026-10-02T00:00:00.000Z' };
    useSelectionStore.getState().setTimeRange(typed.start, typed.end);
    renderHook(() => useLiveRange());

    vi.advanceTimersByTime(3 * REFRESH_MS);
    expect(useSelectionStore.getState().timeRange).toEqual(typed);
  });

  it('stops when it is unmounted', () => {
    const { unmount } = renderHook(() => useLiveRange());
    unmount();
    vi.advanceTimersByTime(REFRESH_MS);
    expect(useSelectionStore.getState().timeRange.end).toBe('2026-10-05T10:00:00.000Z');
  });
});
