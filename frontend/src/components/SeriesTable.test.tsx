import { render, screen, waitFor, within } from '@testing-library/react';
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

  it('lists the series of the metric with a column for each label, and the uuid in small', async () => {
    mockListSeries.mockResolvedValue({
      data: {
        ...catalog,
        'dcat:dataset': [
          { ...dataset('uuid-a', 'alpha'), 'sensor:labels': [{ host: 'alpha', room: 'lab' }] },
          dataset('uuid-b', 'beta'),
        ],
      },
    });
    renderTable();

    await screen.findByText('uuid-a');
    const headers = screen.getAllByRole('columnheader').map((h) => h.textContent);
    expect(headers).toEqual(['Select', 'host', 'room', 'Series ID']);
    const [, alpha, beta] = screen.getAllByRole('row');
    expect(within(alpha).getAllByRole('cell').map((c) => c.textContent)).toEqual(['', 'alpha', 'lab', 'uuid-a']);
    // A label that a series does not have is a dash
    expect(within(beta).getAllByRole('cell').map((c) => c.textContent)).toEqual(['', 'beta', '—', 'uuid-b']);
    // The type is the one of the metric: no column for it
    expect(screen.queryByText('float')).not.toBeInTheDocument();
  });

  it('says the type of each series when the metric has more than one', async () => {
    mockListSeries.mockResolvedValue({
      data: { ...catalog, 'dcat:dataset': [dataset('uuid-a', 'alpha'), { ...dataset('uuid-b', 'beta'), 'sensor:type': 'string' }] },
    });
    renderTable();

    await screen.findByText('uuid-a');
    expect(screen.getByRole('columnheader', { name: 'Type' })).toBeInTheDocument();
    expect(screen.getByText('float')).toBeInTheDocument();
    expect(screen.getByText('string')).toBeInTheDocument();
  });

  describe('selection', () => {
    const uuids = () => useSelectionStore.getState().selectedSeries.map((s) => s.uuid);

    it('selects all the series of a metric that is not too big, with a color each', async () => {
      renderTable();

      await waitFor(() => expect(uuids()).toEqual(['uuid-a', 'uuid-b']));
      expect(useSelectionStore.getState().selectedSeries[0]).toEqual({
        uuid: 'uuid-a',
        name: 'temperature',
        labels: { host: 'alpha' },
        type: 'float',
        slot: 0,
      });
      expect(await screen.findAllByTestId('series-color')).toHaveLength(2);
      expect(screen.getByText('2 selected')).toBeInTheDocument();
    });

    it('does not decide for the user when there are too many series', async () => {
      const many = Array.from({ length: 9 }, (_, i) => dataset(`uuid-${i}`, `host-${i}`));
      mockListSeries.mockResolvedValue({ data: { ...catalog, 'dcat:dataset': many } });
      renderTable();

      await screen.findByText('uuid-0');
      expect(uuids()).toEqual([]);
    });

    it('does not decide either when there is another page', async () => {
      mockListSeries.mockResolvedValue({
        data: {
          ...catalog,
          'hydra:view': { '@type': 'hydra:PartialCollectionView', 'hydra:next': '/series?bookmark=p2', 'hydra:itemsPerPage': 2 },
        },
      });
      renderTable();

      await screen.findByText('uuid-a');
      expect(uuids()).toEqual([]);
    });

    it('leaves what the address selected', async () => {
      useSelectionStore.getState().selectSeries([{ uuid: 'uuid-b', name: 'temperature', labels: { host: 'beta' }, type: 'float' }]);
      renderTable();

      await screen.findByText('uuid-a');
      expect(uuids()).toEqual(['uuid-b']);
    });

    it('does not select what cannot be drawn', async () => {
      mockListSeries.mockResolvedValue({
        data: { ...catalog, 'dcat:dataset': [dataset('uuid-a', 'alpha'), { ...dataset('uuid-b', 'beta'), 'sensor:type': 'string' }] },
      });
      renderTable();

      await waitFor(() => expect(uuids()).toEqual(['uuid-a']));
    });

    it('does it once: what the user clears stays cleared', async () => {
      const user = userEvent.setup();
      renderTable();
      await waitFor(() => expect(uuids()).toHaveLength(2));

      await user.click(screen.getByRole('checkbox', { name: 'Select series uuid-a' }));
      await user.click(screen.getByRole('checkbox', { name: 'Select series uuid-b' }));
      expect(uuids()).toEqual([]);
    });

    it('keeps the color of a series when another one is unselected', async () => {
      const user = userEvent.setup();
      renderTable();
      await waitFor(() => expect(uuids()).toHaveLength(2));

      await user.click(screen.getByRole('checkbox', { name: 'Select series uuid-a' }));
      expect(useSelectionStore.getState().selectedSeries).toMatchObject([{ uuid: 'uuid-b', slot: 1 }]);
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
