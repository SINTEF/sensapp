import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, beforeEach } from 'vitest';
import { TimeRangeSelector } from '../components/TimeRangeSelector';
import { useSelectionStore } from '../stores/useSelectionStore';

describe('TimeRangeSelector', () => {
  beforeEach(() => {
    // Reset store to a known time range
    useSelectionStore.setState({
      timeRange: {
        start: '2025-01-01T00:00:00.000Z',
        end: '2025-01-01T01:00:00.000Z',
      },
      step: 'auto',
      aggregation: 'avg',
    });
  });

  it('renders all preset buttons', () => {
    render(<TimeRangeSelector />);
    for (const label of ['15m', '1h', '6h', '24h', '7d', '30d']) {
      expect(screen.getByRole('button', { name: label })).toBeInTheDocument();
    }
  });

  it('updates time range in store when clicking a preset', async () => {
    const user = userEvent.setup();
    render(<TimeRangeSelector />);

    await user.click(screen.getByRole('button', { name: '1h' }));

    const { timeRange } = useSelectionStore.getState();
    const diffMs = new Date(timeRange.end).getTime() - new Date(timeRange.start).getTime();
    expect(diffMs).toBeCloseTo(60 * 60 * 1000, -3); // ~1 hour
  });

  it('clicking "24h" sets a 24-hour range', async () => {
    const user = userEvent.setup();
    render(<TimeRangeSelector />);

    await user.click(screen.getByRole('button', { name: '24h' }));

    const { timeRange } = useSelectionStore.getState();
    const diffMs = new Date(timeRange.end).getTime() - new Date(timeRange.start).getTime();
    expect(diffMs).toBeCloseTo(24 * 60 * 60 * 1000, -3);
  });

  it('renders from and to datetime inputs', () => {
    render(<TimeRangeSelector />);
    expect(screen.getByText('from')).toBeInTheDocument();
    expect(screen.getByText('to')).toBeInTheDocument();
  });

  it('chooses the step: auto, raw or a duration', async () => {
    const user = userEvent.setup();
    render(<TimeRangeSelector />);
    const step = screen.getByRole('combobox', { name: 'Step' });
    expect(step).toHaveValue('auto');

    await user.selectOptions(step, '5m');
    expect(useSelectionStore.getState().step).toBe('5m');
    await user.selectOptions(step, 'raw');
    expect(useSelectionStore.getState().step).toBe('raw');
  });

  it('chooses the aggregation, which has no use on raw data', async () => {
    const user = userEvent.setup();
    render(<TimeRangeSelector />);
    const aggregation = screen.getByRole('combobox', { name: 'Aggregation' });
    expect(aggregation).toHaveValue('avg');
    expect(aggregation).toBeEnabled();

    await user.selectOptions(aggregation, 'max');
    expect(useSelectionStore.getState().aggregation).toBe('max');

    await user.selectOptions(screen.getByRole('combobox', { name: 'Step' }), 'raw');
    expect(aggregation).toBeDisabled();
  });

  it('marks the preset the range comes from, and none once dates are typed', async () => {
    const user = userEvent.setup();
    render(<TimeRangeSelector />);

    await user.click(screen.getByRole('button', { name: '24h' }));
    expect(useSelectionStore.getState().relativeRange).toBe('24h');
    expect(screen.getByRole('button', { name: '24h' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: '1h' })).toHaveAttribute('aria-pressed', 'false');

    act(() => useSelectionStore.getState().setTimeRange('2026-10-04T10:00:00.000Z', '2026-10-04T11:00:00.000Z'));
    expect(useSelectionStore.getState().relativeRange).toBeNull();
    expect(screen.getByRole('button', { name: '24h' })).toHaveAttribute('aria-pressed', 'false');
  });

  describe('moving the window', () => {
    const typed = { start: '2026-10-04T10:00:00.000Z', end: '2026-10-04T12:00:00.000Z' };
    const range = () => useSelectionStore.getState().timeRange;

    beforeEach(() => {
      useSelectionStore.setState({ rangeHistory: [], relativeRange: null, timeRange: typed });
    });

    it('goes earlier and later by half a window', async () => {
      const user = userEvent.setup();
      render(<TimeRangeSelector />);

      await user.click(screen.getByRole('button', { name: 'Earlier' }));
      expect(range()).toEqual({ start: '2026-10-04T09:00:00.000Z', end: '2026-10-04T11:00:00.000Z' });
      await user.click(screen.getByRole('button', { name: 'Later' }));
      expect(range()).toEqual(typed);
    });

    it('zooms out, and goes back', async () => {
      const user = userEvent.setup();
      render(<TimeRangeSelector />);
      const back = screen.getByRole('button', { name: 'Back to the previous window' });
      expect(back).toBeDisabled();

      await user.click(screen.getByRole('button', { name: 'Zoom out' }));
      expect(range()).toEqual({ start: '2026-10-04T09:00:00.000Z', end: '2026-10-04T13:00:00.000Z' });
      expect(back).toBeEnabled();
      await user.click(back);
      expect(range()).toEqual(typed);
      expect(back).toBeDisabled();
    });

    it('has no later for a live window: it is at now', async () => {
      const user = userEvent.setup();
      render(<TimeRangeSelector />);
      await user.click(screen.getByRole('button', { name: '1h' }));
      expect(screen.getByRole('button', { name: 'Later' })).toBeDisabled();
      expect(screen.getByRole('button', { name: 'Earlier' })).toBeEnabled();
    });
  });
});
