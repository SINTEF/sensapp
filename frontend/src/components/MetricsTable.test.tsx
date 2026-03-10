import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { MetricsTable } from '../components/MetricsTable';
import { useSelectionStore } from '../stores/useSelectionStore';

// Mock the generated API client at the module level
const mockListMetrics = vi.fn();
vi.mock('../client', () => ({
  listMetrics: (...args: unknown[]) => mockListMetrics(...args),
}));

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
  };
}

const sampleMetrics = {
  '@context': {},
  '@type': 'dcat:Catalog',
  '@id': 'test',
  'dct:title': 'Test',
  'dct:description': 'Test catalog',
  'dct:publisher': { '@type': 'foaf:Organization', 'foaf:name': 'SensApp' },
  'dcat:dataset': [
    {
      '@type': 'dcat:Dataset',
      '@id': 'cpu_usage',
      'dct:identifier': 'metric:cpu_usage',
      'dct:title': 'cpu_usage',
      'dct:description': 'CPU usage metric',
      'dcat:keyword': ['metric', 'float', 'host'],
      'sensor:type': 'float',
      'sensor:seriesCount': 3,
      'sensor:labelDimensions': ['host', 'region'],
      'dcat:distribution': [],
    },
    {
      '@type': 'dcat:Dataset',
      '@id': 'memory_total',
      'dct:identifier': 'metric:memory_total',
      'dct:title': 'memory_total',
      'dct:description': 'Memory total',
      'dcat:keyword': ['metric', 'integer'],
      'sensor:type': 'integer',
      'sensor:seriesCount': 1,
      'sensor:labelDimensions': [],
      'dcat:distribution': [],
    },
  ],
};

describe('MetricsTable', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockListMetrics.mockResolvedValue({ data: sampleMetrics });
    // Reset store state
    useSelectionStore.setState({
      selectedMetric: null,
      selectedSeries: [],
      labelFilter: '',
    });
  });

  it('shows loading state while fetching metrics', () => {
    mockListMetrics.mockReturnValue(new Promise(() => {})); // never resolves
    render(<MetricsTable />, { wrapper: createWrapper() });
    expect(screen.getByText('Loading metrics...')).toBeInTheDocument();
  });

  it('renders metrics with type badges and series counts', async () => {
    render(<MetricsTable />, { wrapper: createWrapper() });

    // Wait for metrics to load
    expect(await screen.findByText('cpu_usage')).toBeInTheDocument();
    expect(screen.getByText('memory_total')).toBeInTheDocument();

    // Check type badges are in the table rows
    const table = screen.getByRole('table');
    expect(within(table).getByText('float')).toBeInTheDocument();
    expect(within(table).getByText('integer')).toBeInTheDocument();

    // Check series counts
    expect(screen.getByText('3')).toBeInTheDocument();
    expect(screen.getByText('1')).toBeInTheDocument();

    // Check label dimensions
    expect(screen.getByText('host')).toBeInTheDocument();
    expect(screen.getByText('region')).toBeInTheDocument();
  });

  it('selects a metric when clicking a row and updates store', async () => {
    const user = userEvent.setup();
    render(<MetricsTable />, { wrapper: createWrapper() });

    const cpuRow = await screen.findByText('cpu_usage');
    await user.click(cpuRow);

    expect(useSelectionStore.getState().selectedMetric).toBe('cpu_usage');
  });

  it('deselects a metric when clicking the already-selected row', async () => {
    const user = userEvent.setup();
    useSelectionStore.setState({ selectedMetric: 'cpu_usage' });

    render(<MetricsTable />, { wrapper: createWrapper() });

    const cpuRow = await screen.findByText('cpu_usage');
    await user.click(cpuRow);

    expect(useSelectionStore.getState().selectedMetric).toBeNull();
  });

  it('shows error message with readable text on failure', async () => {
    mockListMetrics.mockResolvedValue({
      data: undefined,
      error: { BadRequest: 'Invalid type filter' },
    });

    render(<MetricsTable />, { wrapper: createWrapper() });

    expect(await screen.findByText(/Failed to load metrics/)).toBeInTheDocument();
    expect(screen.getByText(/Invalid type filter/)).toBeInTheDocument();
  });

  it('shows empty state when no metrics match', async () => {
    mockListMetrics.mockResolvedValue({
      data: { ...sampleMetrics, 'dcat:dataset': [] },
    });

    render(<MetricsTable />, { wrapper: createWrapper() });
    expect(await screen.findByText('No metrics found')).toBeInTheDocument();
  });

  it('highlights the selected metric row', async () => {
    useSelectionStore.setState({ selectedMetric: 'cpu_usage' });

    render(<MetricsTable />, { wrapper: createWrapper() });
    const cpuRow = (await screen.findByText('cpu_usage')).closest('tr')!;
    expect(cpuRow.className).toContain('bg-primary');
  });

  it('passes filter values to the API call', async () => {
    const user = userEvent.setup();
    render(<MetricsTable />, { wrapper: createWrapper() });

    // Wait for initial load
    await screen.findByText('cpu_usage');

    // Type in the search box
    const searchInput = screen.getByPlaceholderText('Filter by name...');
    await user.type(searchInput, 'cpu');

    // The API should be called with the name filter
    // (debounce means we check the latest call)
    const lastCall = mockListMetrics.mock.calls.at(-1);
    expect(lastCall?.[0]?.query?.name).toBe('cpu');
  });
});
