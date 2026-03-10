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
});
