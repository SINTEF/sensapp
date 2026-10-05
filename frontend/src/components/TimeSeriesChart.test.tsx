import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { TimeSeriesChart } from './TimeSeriesChart';
import { SERIES_COLORS } from '../lib/palette';
import { useSelectionStore } from '../stores/useSelectionStore';

const mockGetSeriesData = vi.fn();
vi.mock('../client', () => ({
  getSeriesData: (...args: unknown[]) => mockGetSeriesData(...args),
}));

const START = '2026-10-04T00:00:00.000Z';
const hours = (n: number) => new Date(Date.parse(START) + n * 3600_000).toISOString();

const temperature = { uuid: 'uuid-temperature', name: 'temperature', labels: {}, type: 'float', slot: 0 };
const state = { uuid: 'uuid-state', name: 'state', labels: {}, type: 'string', slot: 0 };
const door = { uuid: 'uuid-door', name: 'door', labels: {}, type: 'boolean', slot: 0 };

function renderChart() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<TimeSeriesChart />, {
    wrapper: ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    ),
  });
}

function queryOf(uuid: string) {
  return mockGetSeriesData.mock.calls
    .map(([options]) => options)
    .find((options) => options.path.series_uuid === uuid)?.query;
}

describe('TimeSeriesChart', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    useSelectionStore.setState({ step: 'auto', aggregation: 'avg', chartStyle: 'line', logScale: false });
    // SenML: the base time is in the first record, the others are relative to it
    mockGetSeriesData.mockResolvedValue({
      data: [{ bt: 1_791_000_000, t: 0, v: 1 }, { t: 30, v: 2 }, { t: 60, v: 3 }],
    });
  });

  it('averages the numeric series of a wide range, the server refuses to send them raw', async () => {
    useSelectionStore.setState({
      selectedSeries: [temperature, state],
      timeRange: { start: START, end: hours(24) },
    });
    renderChart();

    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(2));
    expect(queryOf('uuid-temperature')).toMatchObject({ step: '1m', aggregation: 'avg' });
    // Not the others: strings cannot be averaged
    expect(queryOf('uuid-state')).not.toHaveProperty('step');
    expect(queryOf('uuid-state')).not.toHaveProperty('aggregation');
  });

  it('sends the step and the aggregation that were chosen, to the numeric series only', async () => {
    useSelectionStore.setState({
      selectedSeries: [temperature, state],
      timeRange: { start: START, end: hours(1) },
      step: '5m',
      aggregation: 'max',
    });
    renderChart();

    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(2));
    expect(queryOf('uuid-temperature')).toMatchObject({ step: '5m', aggregation: 'max' });
    expect(queryOf('uuid-state')).not.toHaveProperty('step');
  });

  it('reads raw when asked to, even on a wide range', async () => {
    useSelectionStore.setState({
      selectedSeries: [temperature],
      timeRange: { start: START, end: hours(24) },
      step: 'raw',
    });
    renderChart();

    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(1));
    expect(queryOf('uuid-temperature')).not.toHaveProperty('step');
    expect(queryOf('uuid-temperature')).not.toHaveProperty('aggregation');
  });

  it('asks again when the step or the aggregation changes', async () => {
    useSelectionStore.setState({
      selectedSeries: [temperature],
      timeRange: { start: START, end: hours(1) },
      step: '5m',
      aggregation: 'avg',
    });
    renderChart();
    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(1));

    act(() => useSelectionStore.getState().setAggregation('min'));
    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(2));
    expect(mockGetSeriesData.mock.lastCall?.[0].query).toMatchObject({ aggregation: 'min' });

    act(() => useSelectionStore.getState().setStep('1h'));
    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(3));
    expect(mockGetSeriesData.mock.lastCall?.[0].query).toMatchObject({ step: '1h' });
  });

  it('reads a short range as it is', async () => {
    useSelectionStore.setState({
      selectedSeries: [temperature],
      timeRange: { start: START, end: hours(0.25) },
    });
    renderChart();

    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(1));
    expect(queryOf('uuid-temperature')).toEqual({
      format: 'senml',
      start: START,
      end: hours(0.25),
    });
  });

  it('draws one line per series', async () => {
    useSelectionStore.setState({
      selectedSeries: [temperature],
      timeRange: { start: START, end: hours(1) },
    });
    renderChart();

    expect(await screen.findByTestId('echarts')).toBeInTheDocument();
    await waitFor(() => expect(screen.getByTestId('echarts')).toHaveAttribute('data-series-count', '1'));
  });

  it('says which series failed, and draws the others', async () => {
    mockGetSeriesData.mockImplementation(({ path }) =>
      Promise.resolve(
        path.series_uuid === 'uuid-state'
          ? { error: { error: 'Internal Server Error' }, response: { status: 500 } }
          : { data: [{ bt: 1_791_000_000, t: 0, v: 1 }] },
      ),
    );
    useSelectionStore.setState({
      selectedSeries: [temperature, state],
      timeRange: { start: START, end: hours(1) },
    });
    renderChart();

    expect(await screen.findByText(/Some series failed to load: Internal Server Error/)).toBeInTheDocument();
    await waitFor(() => expect(screen.getByTestId('echarts')).toHaveAttribute('data-series-count', '1'));
  });

  it('draws a boolean as 0 and 1 on an axis of its own, and reads it raw', async () => {
    mockGetSeriesData.mockResolvedValue({
      data: [{ bt: 1_791_000_000, t: 0, vb: true }, { t: 30, vb: false }, { t: 60, vb: true }],
    });
    useSelectionStore.setState({
      selectedSeries: [door],
      timeRange: { start: START, end: hours(24) },
    });
    renderChart();

    const chart = await screen.findByTestId('echarts');
    await waitFor(() => expect(chart).toHaveAttribute('data-series-count', '1'));
    const option = JSON.parse(chart.getAttribute('data-option')!);
    expect(option.series[0]).toMatchObject({ step: 'end', yAxisIndex: 1 });
    expect(option.series[0].data.map(([, value]: [number, number]) => value)).toEqual([1, 0, 1]);
    // A boolean cannot be averaged, even on a wide range
    expect(queryOf('uuid-door')).not.toHaveProperty('step');
  });

  it('draws with the style and the scale that were chosen', async () => {
    useSelectionStore.setState({
      selectedSeries: [temperature],
      timeRange: { start: START, end: hours(1) },
      chartStyle: 'stacked',
      logScale: true,
    });
    renderChart();

    const chart = await screen.findByTestId('echarts');
    await waitFor(() => expect(chart).toHaveAttribute('data-series-count', '1'));
    const option = JSON.parse(chart.getAttribute('data-option')!);
    expect(option.series[0]).toMatchObject({ stack: 'total' });
    expect(option.yAxis[0]).toMatchObject({ type: 'log' });
  });

  it('keeps the series drawn while the data of a moved range is on its way', async () => {
    useSelectionStore.setState({
      selectedSeries: [temperature],
      timeRange: { start: START, end: hours(1) },
    });
    renderChart();
    await waitFor(() => expect(screen.getByTestId('echarts')).toHaveAttribute('data-series-count', '1'));

    mockGetSeriesData.mockReturnValue(new Promise(() => {}));
    act(() => useSelectionStore.getState().setTimeRange(hours(0.1), hours(1.1)));
    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(2));

    expect(screen.getByTestId('echarts')).toHaveAttribute('data-series-count', '1');
  });

  it('draws each series with the color of its slot, the one of its swatch in the list', async () => {
    useSelectionStore.setState({
      selectedSeries: [{ ...temperature, slot: 3 }],
      timeRange: { start: START, end: hours(1) },
    });
    renderChart();

    const chart = await screen.findByTestId('echarts');
    await waitFor(() => expect(chart).toHaveAttribute('data-series-count', '1'));
    const option = JSON.parse(chart.getAttribute('data-option')!);
    expect(option.series[0]).toMatchObject({ id: 'uuid-temperature', color: SERIES_COLORS.light[3] });
    expect(option.legend).toBeUndefined();
  });

  it('shows the window that was brushed on the chart, and reads it again', async () => {
    const user = userEvent.setup();
    useSelectionStore.setState({
      selectedSeries: [temperature],
      timeRange: { start: START, end: hours(1) },
      relativeRange: null,
    });
    renderChart();
    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(1));

    await user.click(await screen.findByTestId('echarts-brush'));

    // In whole seconds, outwards
    expect(useSelectionStore.getState().timeRange).toEqual({
      start: '2026-10-04T00:10:00.000Z',
      end: '2026-10-04T00:20:01.000Z',
    });
    expect(useSelectionStore.getState().relativeRange).toBeNull();
    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(2));
    expect(mockGetSeriesData.mock.lastCall?.[0].query).toMatchObject({
      start: '2026-10-04T00:10:00.000Z',
      end: '2026-10-04T00:20:01.000Z',
    });
  });

  it('says it is loading without moving the chart', async () => {
    mockGetSeriesData.mockReturnValue(new Promise(() => {}));
    useSelectionStore.setState({ selectedSeries: [temperature], timeRange: { start: START, end: hours(1) } });
    renderChart();

    expect(await screen.findByRole('progressbar', { name: 'Loading chart data' })).toBeInTheDocument();
    expect(screen.getByTestId('echarts')).toBeInTheDocument();
  });
});
