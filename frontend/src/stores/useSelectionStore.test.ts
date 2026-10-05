import { renderHook, act } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { useSelectionStore } from '../stores/useSelectionStore';

describe('useSelectionStore', () => {
  beforeEach(() => {
    const { setSelectedMetric, clearSelectedSeries, setLabelFilter } =
      useSelectionStore.getState();
    setSelectedMetric(null);
    clearSelectedSeries();
    setLabelFilter('');
  });

  it('should have sensible default state', () => {
    const { result } = renderHook(() => useSelectionStore());
    expect(result.current.selectedMetric).toBeNull();
    expect(result.current.selectedSeries).toEqual([]);
    expect(result.current.labelFilter).toBe('');
    // Default time range should be approximately 1 hour ending at "now"
    const start = new Date(result.current.timeRange.start).getTime();
    const end = new Date(result.current.timeRange.end).getTime();
    expect(end - start).toBeCloseTo(60 * 60 * 1000, -3);
  });

  it('should clear series selection when switching metrics', () => {
    const { result } = renderHook(() => useSelectionStore());

    // Select a series first
    act(() => {
      result.current.toggleSeries({
        uuid: 'uuid-1',
        name: 'cpu',
        labels: { host: 'a' },
        type: 'float',
      });
    });
    expect(result.current.selectedSeries).toHaveLength(1);

    // Switching metric should clear selected series
    act(() => {
      result.current.setSelectedMetric('memory');
    });
    expect(result.current.selectedMetric).toBe('memory');
    expect(result.current.selectedSeries).toEqual([]);
  });

  it('should toggle series on and off by uuid', () => {
    const { result } = renderHook(() => useSelectionStore());

    const series1 = { uuid: 'uuid-1', name: 'cpu', labels: { host: 'a' }, type: 'float' };
    const series2 = { uuid: 'uuid-2', name: 'cpu', labels: { host: 'b' }, type: 'float' };

    // Add two series
    act(() => {
      result.current.toggleSeries(series1);
    });
    act(() => {
      result.current.toggleSeries(series2);
    });
    expect(result.current.selectedSeries).toHaveLength(2);

    // Remove the first by toggling again
    act(() => {
      result.current.toggleSeries(series1);
    });
    expect(result.current.selectedSeries).toHaveLength(1);
    expect(result.current.selectedSeries[0].uuid).toBe('uuid-2');
  });

  it('should deselect metric when clicking the same one', () => {
    const { result } = renderHook(() => useSelectionStore());

    act(() => {
      result.current.setSelectedMetric('cpu');
    });
    expect(result.current.selectedMetric).toBe('cpu');

    act(() => {
      result.current.setSelectedMetric(null);
    });
    expect(result.current.selectedMetric).toBeNull();
  });

  it('should update time range independently', () => {
    const { result } = renderHook(() => useSelectionStore());

    act(() => {
      result.current.setTimeRange('2025-01-01T00:00:00Z', '2025-01-02T00:00:00Z');
    });
    expect(result.current.timeRange.start).toBe('2025-01-01T00:00:00Z');
    expect(result.current.timeRange.end).toBe('2025-01-02T00:00:00Z');
  });

  it('clearSelectedSeries should empty the array', () => {
    const { result } = renderHook(() => useSelectionStore());

    act(() => {
      result.current.toggleSeries({ uuid: '1', name: 'a', labels: {}, type: 'float' });
      result.current.toggleSeries({ uuid: '2', name: 'b', labels: {}, type: 'float' });
    });
    expect(result.current.selectedSeries).toHaveLength(2);

    act(() => {
      result.current.clearSelectedSeries();
    });
    expect(result.current.selectedSeries).toHaveLength(0);
  });

  describe('colors', () => {
    const info = (uuid: string) => ({ uuid, name: 'cpu', labels: {}, type: 'float' });
    const slots = () => useSelectionStore.getState().selectedSeries.map((s) => [s.uuid, s.slot]);

    it('keeps the color of a series as long as it is selected', () => {
      const { toggleSeries } = useSelectionStore.getState();
      act(() => ['a', 'b', 'c'].forEach((uuid) => toggleSeries(info(uuid))));
      expect(slots()).toEqual([['a', 0], ['b', 1], ['c', 2]]);

      // The first one goes: the others keep theirs, and the next one takes the free one
      act(() => toggleSeries(info('a')));
      expect(slots()).toEqual([['b', 1], ['c', 2]]);
      act(() => toggleSeries(info('d')));
      expect(slots()).toEqual([['b', 1], ['c', 2], ['d', 0]]);
    });

    it('selects many series at once, those that are already selected staying as they are', () => {
      const { toggleSeries, selectSeries } = useSelectionStore.getState();
      act(() => toggleSeries(info('b')));
      act(() => selectSeries([info('a'), info('b'), info('c')]));
      expect(slots()).toEqual([['b', 0], ['a', 1], ['c', 2]]);
    });
  });

  describe('the window', () => {
    const typed = { start: '2026-10-01T00:00:00.000Z', end: '2026-10-01T01:00:00.000Z' };
    const state = () => useSelectionStore.getState();

    beforeEach(() => {
      useSelectionStore.setState({ rangeHistory: [] });
      state().setRelativeRange('1h');
      useSelectionStore.setState({ rangeHistory: [] });
    });

    it('goes back to the windows the user left, the last one first', () => {
      act(() => state().setTimeRange(typed.start, typed.end));
      act(() => state().panRange(-1));
      expect(state().timeRange.start).toBe('2026-09-30T23:30:00.000Z');

      act(() => state().undoRange());
      expect(state().timeRange).toEqual(typed);
      act(() => state().undoRange());
      // The first window was the last hour: it is the last hour as of now
      expect(state().relativeRange).toBe('1h');
      expect(Date.parse(state().timeRange.end)).toBeGreaterThan(Date.now() - 5000);
      expect(state().rangeHistory).toEqual([]);
      // Nothing more to go back to
      act(() => state().undoRange());
      expect(state().relativeRange).toBe('1h');
    });

    it('does not keep what the clock does, nor the same preset again', () => {
      act(() => state().refreshRelativeRange());
      act(() => state().setRelativeRange('1h'));
      expect(state().rangeHistory).toEqual([]);
      act(() => state().setRelativeRange('6h'));
      expect(state().rangeHistory).toHaveLength(1);
    });

    it('remembers 20 windows', () => {
      for (let i = 0; i < 25; i++) act(() => state().panRange(-1));
      expect(state().rangeHistory).toHaveLength(20);
    });

    it('pans to a window of dates, and zooms out of it', () => {
      act(() => state().panRange(-1));
      expect(state().relativeRange).toBeNull();
      act(() => state().setTimeRange(typed.start, typed.end));
      act(() => state().zoomOutRange());
      expect(state().timeRange).toEqual({ start: '2026-09-30T23:30:00.000Z', end: '2026-10-01T01:30:00.000Z' });
    });

    it('zooms out of a live window into the next preset, still live', () => {
      act(() => state().zoomOutRange());
      expect(state().relativeRange).toBe('6h');
      const hours = (Date.parse(state().timeRange.end) - Date.parse(state().timeRange.start)) / 3_600_000;
      expect(hours).toBe(6);
      act(() => state().setRelativeRange('30d'));
      const before = state().rangeHistory.length;
      act(() => state().zoomOutRange());
      expect(state().relativeRange).toBe('30d');
      expect(state().rangeHistory).toHaveLength(before);
    });
  });
});
