import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { SeriesTable } from './SeriesTable';
import { useSelectionStore } from '../stores/useSelectionStore';

const mockListSeries = vi.fn();
vi.mock('../client', () => ({
  listSeries: (...args: unknown[]) => mockListSeries(...args),
}));

const dataset = (uuid: string, host: string) => ({
  '@type': 'dcat:Dataset',
  '@id': `temperature{host="${host}"}`,
  'dct:identifier': uuid,
  'dct:title': 'temperature',
  'sensor:type': 'float',
  'sensor:labels': [{ host }],
  'dcat:distribution': [],
});

const catalog = {
  '@type': 'dcat:Catalog',
  'dcat:dataset': [dataset('uuid-a', 'alpha'), dataset('uuid-b', 'beta')],
};

function renderTable() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<SeriesTable />, {
    wrapper: ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    ),
  });
}

describe('SeriesTable', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockListSeries.mockResolvedValue({ data: catalog });
    useSelectionStore.setState({ selectedMetric: 'temperature', selectedSeries: [], labelFilter: '' });
  });

  it('lists the series of the metric and selects them', async () => {
    const user = userEvent.setup();
    renderTable();

    await user.click(await screen.findByRole('checkbox', { name: 'Select series uuid-a' }));
    expect(useSelectionStore.getState().selectedSeries).toEqual([
      { uuid: 'uuid-a', name: 'temperature', labels: { host: 'alpha' }, type: 'float' },
    ]);
    expect(mockListSeries).toHaveBeenCalledWith({ query: { metric: 'temperature', selector: undefined } });
  });

  it('filters the list as people type', async () => {
    const user = userEvent.setup();
    renderTable();
    await screen.findByText('uuid-a');

    await user.type(screen.getByPlaceholderText('Quick filter...'), 'beta');
    expect(screen.queryByText('uuid-a')).not.toBeInTheDocument();
    expect(screen.getByText('uuid-b')).toBeInTheDocument();
  });

  it('says when the list is cut, the server sends 256 series at most', async () => {
    mockListSeries.mockResolvedValue({
      data: { ...catalog, 'hydra:view': { '@type': 'hydra:PartialCollectionView', 'hydra:next': '/series?bookmark=x', 'hydra:itemsPerPage': 2 } },
    });
    renderTable();
    expect(await screen.findByText(/more series exist/)).toBeInTheDocument();
  });

  it('does not say it when the list is complete', async () => {
    renderTable();
    await screen.findByText('uuid-a');
    expect(screen.queryByText(/more series exist/)).not.toBeInTheDocument();
  });

  it('shows what failed', async () => {
    mockListSeries.mockResolvedValue({ error: { error: 'Selector is invalid' }, response: { status: 400 } });
    renderTable();
    expect(await screen.findByText(/Failed to load series: Selector is invalid/)).toBeInTheDocument();
  });
});
