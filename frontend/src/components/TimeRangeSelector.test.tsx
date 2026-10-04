import { render, screen } from '@testing-library/react';
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
});
