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
    expect(mockListSeries).toHaveBeenCalledWith({
      query: { metric: 'temperature', selector: undefined, bookmark: undefined },
    });
  });

  it('filters the list as people type', async () => {
    const user = userEvent.setup();
    renderTable();
    await screen.findByText('uuid-a');

    await user.type(screen.getByPlaceholderText('Quick filter...'), 'beta');
    expect(screen.queryByText('uuid-a')).not.toBeInTheDocument();
    expect(screen.getByText('uuid-b')).toBeInTheDocument();
  });

  describe('pages', () => {
    const page = (uuid: string, host: string, bookmark?: string) => ({
      '@type': 'dcat:Catalog',
      'dcat:dataset': [dataset(uuid, host)],
      ...(bookmark && {
        'hydra:view': {
          '@type': 'hydra:PartialCollectionView',
          'hydra:next': `/series?limit=1&bookmark=${bookmark}&metric=temperature`,
          'hydra:itemsPerPage': 1,
        },
      }),
    });

    beforeEach(() => {
      mockListSeries.mockImplementation(({ query }) =>
        Promise.resolve({
          data:
            query.bookmark === 'p2'
              ? page('uuid-2', 'beta', 'p3')
              : query.bookmark === 'p3'
                ? page('uuid-3', 'gamma')
                : page('uuid-1', 'alpha', 'p2'),
        }),
      );
    });

    it('goes forward with the bookmark of the server, and back', async () => {
      const user = userEvent.setup();
      renderTable();
      await screen.findByText('uuid-1');
      expect(screen.getByRole('button', { name: 'Previous page' })).toBeDisabled();

      await user.click(screen.getByRole('button', { name: 'Next page' }));
      expect(await screen.findByText('uuid-2')).toBeInTheDocument();
      expect(mockListSeries).toHaveBeenLastCalledWith({
        query: { metric: 'temperature', selector: undefined, bookmark: 'p2' },
      });

      await user.click(screen.getByRole('button', { name: 'Next page' }));
      expect(await screen.findByText('uuid-3')).toBeInTheDocument();
      // The last page has no next
      expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled();

      await user.click(screen.getByRole('button', { name: 'Previous page' }));
      expect(await screen.findByText('uuid-2')).toBeInTheDocument();
      await user.click(screen.getByRole('button', { name: 'Previous page' }));
      expect(await screen.findByText('uuid-1')).toBeInTheDocument();
    });

    it('keeps the selection from one page to the other', async () => {
      const user = userEvent.setup();
      renderTable();
      await user.click(await screen.findByRole('checkbox', { name: 'Select series uuid-1' }));
      await user.click(screen.getByRole('button', { name: 'Next page' }));
      await user.click(await screen.findByRole('checkbox', { name: 'Select series uuid-2' }));
      expect(useSelectionStore.getState().selectedSeries.map((s) => s.uuid)).toEqual(['uuid-1', 'uuid-2']);
    });

    it('starts again from the first page when the selector changes', async () => {
      const user = userEvent.setup();
      renderTable();
      await screen.findByText('uuid-1');
      await user.click(screen.getByRole('button', { name: 'Next page' }));
      await screen.findByText('uuid-2');

      await user.type(screen.getByPlaceholderText(/env=/), 'x');
      expect(await screen.findByText('uuid-1')).toBeInTheDocument();
      expect(mockListSeries.mock.lastCall?.[0].query.bookmark).toBeUndefined();
    });

    it('has no pager when everything fits on one page', async () => {
      mockListSeries.mockResolvedValue({ data: catalog });
      renderTable();
      await screen.findByText('uuid-a');
      expect(screen.queryByRole('button', { name: 'Next page' })).not.toBeInTheDocument();
    });
  });

  it('shows what failed', async () => {
    mockListSeries.mockResolvedValue({ error: { error: 'Selector is invalid' }, response: { status: 400 } });
    renderTable();
    expect(await screen.findByText(/Failed to load series: Selector is invalid/)).toBeInTheDocument();
  });
});
