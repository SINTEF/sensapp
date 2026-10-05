import { act, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, useLocation } from 'react-router-dom';
import type { ReactNode } from 'react';
import { createQueryClient } from '../api/queryClient';
import { useAuthStore } from '../stores/useAuthStore';
import { useSelectionStore } from '../stores/useSelectionStore';
import { useUrlState } from './useUrlState';

const mockGetSeriesData = vi.fn();
vi.mock('../client', () => ({
  getSeriesData: (...args: unknown[]) => mockGetSeriesData(...args),
}));

function Probe() {
  useUrlState();
  const { search } = useLocation();
  return <output data-testid="search">{search}</output>;
}

function renderAt(url: string) {
  const queryClient = createQueryClient();
  queryClient.setDefaultOptions({ queries: { staleTime: 30_000, retry: false } });
  const view = render(<Probe />, {
    wrapper: ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={[url]}>{children}</MemoryRouter>
      </QueryClientProvider>
    ),
  });
  return { ...view, queryClient };
}

const search = () => new URLSearchParams(screen.getByTestId('search').textContent ?? '');

/** The first record of a series, as SensApp sends it. */
const firstRecord = (uuid: string) =>
  uuid === 'uuid-door'
    ? { _name: 'door', bn: uuid, bt: 1, t: 0, vb: true }
    : { _name: 'temperature', _labels: { room: 'lab' }, bn: uuid, bt: 1, t: 0, v: 21.5 };

describe('useUrlState', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    sessionStorage.clear();
    useAuthStore.setState({ token: null, authRequired: false, dialogOpen: false, message: null });
    useSelectionStore.setState({
      selectedMetric: null,
      selectedSeries: [],
      relativeRange: '1h',
      chartStyle: 'line',
      logScale: false,
      step: 'auto',
      aggregation: 'avg',
    });
    mockGetSeriesData.mockImplementation(({ path }) => Promise.resolve({ data: [firstRecord(path.series_uuid)] }));
  });

  it('keeps the plain address of the plain explorer', async () => {
    renderAt('/');
    await waitFor(() => expect(screen.getByTestId('search')).toBeInTheDocument());
    expect(screen.getByTestId('search')).toHaveTextContent('');
    expect(mockGetSeriesData).not.toHaveBeenCalled();
  });

  it('shows what the address says', async () => {
    renderAt('/?metric=temperature&series=uuid-t&series=uuid-door&range=24h&step=5m&agg=max&style=area&log=1');

    await waitFor(() => expect(useSelectionStore.getState().selectedSeries).toHaveLength(2));
    const state = useSelectionStore.getState();
    expect(state).toMatchObject({
      selectedMetric: 'temperature',
      relativeRange: '24h',
      step: '5m',
      aggregation: 'max',
      chartStyle: 'area',
      logScale: true,
    });
    // The name, the labels and the type come from the first sample of each series, a color slot each
    expect(state.selectedSeries).toEqual([
      { uuid: 'uuid-t', name: 'temperature', labels: { room: 'lab' }, type: 'float', slot: 0 },
      { uuid: 'uuid-door', name: 'door', labels: {}, type: 'boolean', slot: 1 },
    ]);
    const hours = (Date.parse(state.timeRange.end) - Date.parse(state.timeRange.start)) / 3_600_000;
    expect(hours).toBe(24);
  });

  it('shows an absolute range as it is', async () => {
    renderAt('/?from=2026-10-04T10:00:00.000Z&to=2026-10-04T11:00:00.000Z');
    await waitFor(() => expect(useSelectionStore.getState().relativeRange).toBeNull());
    expect(useSelectionStore.getState().timeRange).toEqual({
      start: '2026-10-04T10:00:00.000Z',
      end: '2026-10-04T11:00:00.000Z',
    });
  });

  it('keeps the address in step with the explorer, and not in the history', async () => {
    renderAt('/?series=uuid-t&metric=temperature');
    await waitFor(() => expect(useSelectionStore.getState().selectedSeries).toHaveLength(1));

    act(() => {
      useSelectionStore.getState().setStep('1h');
      useSelectionStore.getState().setChartStyle('bars');
      useSelectionStore.getState().setRelativeRange('7d');
    });
    await waitFor(() => expect(search().get('range')).toBe('7d'));
    expect(search().get('step')).toBe('1h');
    expect(search().get('style')).toBe('bars');
    expect(search().getAll('series')).toEqual(['uuid-t']);
    expect(search().get('metric')).toBe('temperature');

    act(() => useSelectionStore.getState().clearSelectedSeries());
    await waitFor(() => expect(search().has('series')).toBe(false));
  });

  it('writes the dates once they were typed', async () => {
    renderAt('/');
    await waitFor(() => expect(screen.getByTestId('search')).toBeInTheDocument());
    act(() => useSelectionStore.getState().setTimeRange('2026-10-04T10:00:00.000Z', '2026-10-04T11:00:00.000Z'));
    await waitFor(() => expect(search().get('from')).toBe('2026-10-04T10:00:00.000Z'));
    expect(search().has('range')).toBe(false);
  });

  it('forgets a series that is gone, and the nonsense of the address', async () => {
    mockGetSeriesData.mockImplementation(({ path }) =>
      Promise.resolve(
        path.series_uuid === 'uuid-gone'
          ? { error: { error: 'not found' }, response: { status: 404 } }
          : { data: [firstRecord(path.series_uuid)] },
      ),
    );
    renderAt('/?series=uuid-gone&series=uuid-t&step=7x&style=pie');
    await waitFor(() => expect(useSelectionStore.getState().selectedSeries.map((s) => s.uuid)).toEqual(['uuid-t']));
    expect(useSelectionStore.getState()).toMatchObject({ step: 'auto', chartStyle: 'line' });
    await waitFor(() => expect(search().getAll('series')).toEqual(['uuid-t']));
    expect(search().has('step')).toBe(false);
  });

  it('asks for a token on a shared link, then shows the series', async () => {
    mockGetSeriesData.mockImplementation(({ path }) =>
      Promise.resolve(
        useAuthStore.getState().token
          ? { data: [firstRecord(path.series_uuid)] }
          : { error: { error: 'Missing or invalid Authorization header' }, response: { status: 401 } },
      ),
    );
    const { queryClient } = renderAt('/?series=uuid-t');

    await waitFor(() => expect(useAuthStore.getState().dialogOpen).toBe(true));
    // Nothing is written while the series are unknown: the link must not be lost
    expect(search().getAll('series')).toEqual(['uuid-t']);
    expect(useSelectionStore.getState().selectedSeries).toEqual([]);

    // What the token dialog does
    act(() => useAuthStore.getState().signIn('a.token.here'));
    await act(() => queryClient.invalidateQueries());

    await waitFor(() => expect(useSelectionStore.getState().selectedSeries.map((s) => s.uuid)).toEqual(['uuid-t']));
    expect(search().getAll('series')).toEqual(['uuid-t']);
  });
});
