import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { SeriesTable } from './SeriesTable';
import { SERIES_COLORS } from '../lib/palette';
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
    expect(screen.getAllByTitle(/^Sort by/).map((b) => b.title)).toEqual(['Sort by host', 'Sort by room']);
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

  describe('sorting', () => {
    const hosts = () =>
      screen
        .getAllByRole('row')
        .slice(1)
        .map((row) => within(row).getAllByRole('cell')[1].textContent);
    const withHosts = (...names: string[]) =>
      mockListSeries.mockResolvedValue({
        data: { ...catalog, 'dcat:dataset': names.map((name, i) => dataset(`uuid-${i}`, name)) },
      });

    it('sorts by the first column, numbers in natural order', async () => {
      withHosts('node-10', 'node-2', 'node-1');
      renderTable();
      await screen.findByText('uuid-0');
      expect(hosts()).toEqual(['node-1', 'node-2', 'node-10']);
      expect(screen.getByRole('columnheader', { name: /host/ })).toHaveAttribute('aria-sort', 'ascending');
    });

    it('sorts by the column that is clicked, and the other way when it is clicked again', async () => {
      const user = userEvent.setup();
      mockListSeries.mockResolvedValue({
        data: {
          ...catalog,
          'dcat:dataset': [
            { ...dataset('uuid-a', 'alpha'), 'sensor:labels': [{ host: 'alpha', room: 'z' }] },
            { ...dataset('uuid-b', 'beta'), 'sensor:labels': [{ host: 'beta', room: 'a' }] },
          ],
        },
      });
      renderTable();
      await screen.findByText('uuid-a');
      expect(hosts()).toEqual(['alpha', 'beta']);

      await user.click(screen.getByTitle('Sort by room'));
      expect(hosts()).toEqual(['beta', 'alpha']);
      await user.click(screen.getByTitle('Sort by room'));
      expect(hosts()).toEqual(['alpha', 'beta']);
      expect(screen.getByRole('columnheader', { name: /room/ })).toHaveAttribute('aria-sort', 'descending');
      await user.click(screen.getByTitle('Sort by host'));
      expect(screen.getByRole('columnheader', { name: /host/ })).toHaveAttribute('aria-sort', 'ascending');
    });
  });

  describe('labels that are on every series', () => {
    const influx = (host: string) => ({
      ...dataset(`uuid-${host}`, host),
      'sensor:labels': [{ influxdb_org: 'sensapp', host }],
    });

    it('are said once above the list, not in a column', async () => {
      mockListSeries.mockResolvedValue({ data: { ...catalog, 'dcat:dataset': [influx('alpha'), influx('beta')] } });
      renderTable();

      await screen.findByText('uuid-alpha');
      expect(screen.getByText('on every series')).toBeInTheDocument();
      expect(screen.getByText('influxdb_org=')).toBeInTheDocument();
      expect(screen.queryByTitle('Sort by influxdb_org')).not.toBeInTheDocument();
      expect(screen.getByTitle('Sort by host')).toBeInTheDocument();
    });

    it('stay a column when there is one series: it would have nothing else to show', async () => {
      mockListSeries.mockResolvedValue({ data: { ...catalog, 'dcat:dataset': [influx('alpha')] } });
      renderTable();

      await screen.findByText('uuid-alpha');
      expect(screen.queryByText('on every series')).not.toBeInTheDocument();
      expect(screen.getByTitle('Sort by influxdb_org')).toBeInTheDocument();
    });
  });

  it('suggests a selector made of labels of the series', async () => {
    mockListSeries.mockResolvedValue({
      data: {
        ...catalog,
        'dcat:dataset': [{ ...dataset('uuid-a', 'alpha'), 'sensor:labels': [{ host: 'alpha', room: 'lab' }] }],
      },
    });
    renderTable();
    await screen.findByText('uuid-a');
    expect(screen.getByPlaceholderText('{host="alpha", room=~"l.*"}')).toBeInTheDocument();
  });

  it('tells the chart which series the pointer is on, and forgets it when it leaves', async () => {
    const user = userEvent.setup();
    renderTable();
    await screen.findByText('uuid-a');
    const row = screen.getByText('uuid-b').closest('tr')!;

    await user.hover(row);
    expect(useSelectionStore.getState().hoveredSeries).toBe('uuid-b');
    await user.unhover(row);
    expect(useSelectionStore.getState().hoveredSeries).toBeNull();
  });

  describe('selection', () => {
    const uuids = () => useSelectionStore.getState().selectedSeries.map((s) => s.uuid);

    it('selects, and unselects, what is on screen from the header', async () => {
      const user = userEvent.setup();
      const many = Array.from({ length: 10 }, (_, i) => dataset(`uuid-${i}`, `host-${i}`));
      mockListSeries.mockResolvedValue({ data: { ...catalog, 'dcat:dataset': many } });
      renderTable();
      await screen.findByText('uuid-0');
      const all = screen.getByRole('checkbox', { name: 'Select all series' });
      expect(all).not.toBeChecked();

      await user.click(all);
      expect(uuids()).toHaveLength(10);
      // Ten series, ten colors: none repeated
      expect(new Set(useSelectionStore.getState().selectedSeries.map((s) => s.slot)).size).toBe(10);
      expect(all).toBeChecked();

      await user.click(screen.getByRole('checkbox', { name: 'Select series uuid-3' }));
      expect(all).not.toBeChecked();
      expect((all as HTMLInputElement).indeterminate).toBe(true);

      await user.click(all);
      expect(uuids()).toHaveLength(10);
      await user.click(all);
      expect(uuids()).toEqual([]);
    });

    it('selects only what the quick filter lets through', async () => {
      const user = userEvent.setup();
      const many = Array.from({ length: 10 }, (_, i) => dataset(`uuid-${i}`, i < 3 ? `lab-${i}` : `other-${i}`));
      mockListSeries.mockResolvedValue({ data: { ...catalog, 'dcat:dataset': many } });
      renderTable();
      await screen.findByText('uuid-0');

      await user.type(screen.getByPlaceholderText('Quick filter...'), 'lab');
      await user.click(screen.getByRole('checkbox', { name: 'Select all series' }));
      expect(uuids().sort()).toEqual(['uuid-0', 'uuid-1', 'uuid-2']);
    });

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
      // The checkbox of a series has the color of its line
      const first = screen.getByRole('checkbox', { name: 'Select series uuid-a' });
      const second = screen.getByRole('checkbox', { name: 'Select series uuid-b' });
      expect(first).toBeChecked();
      expect(first.style.getPropertyValue('--input-color')).toBe(SERIES_COLORS.light[0]);
      expect(second.style.getPropertyValue('--input-color')).toBe(SERIES_COLORS.light[1]);
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

      await user.type(screen.getByPlaceholderText(/host=/), 'x');
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
