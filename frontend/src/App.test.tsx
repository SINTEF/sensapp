import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import type { ReactNode } from 'react';
import App from './App';
import { useSelectionStore } from './stores/useSelectionStore';

// Mock the API client with controllable responses
const mockListMetrics = vi.fn();
const mockListSeries = vi.fn();
const mockReadiness = vi.fn();
const mockGetSeriesData = vi.fn();

vi.mock('./client', () => ({
  listMetrics: (...args: unknown[]) => mockListMetrics(...args),
  listSeries: (...args: unknown[]) => mockListSeries(...args),
  readiness: (...args: unknown[]) => mockReadiness(...args),
  getSeriesData: (...args: unknown[]) => mockGetSeriesData(...args),
}));

const emptyCatalog = {
  '@context': {},
  '@type': 'dcat:Catalog',
  '@id': 'sensapp_metrics_catalog',
  'dct:title': 'SensApp Metrics Catalog',
  'dcat:dataset': [],
};

const catalogWithMetrics = {
  ...emptyCatalog,
  'dcat:dataset': [
    {
      '@type': 'dcat:Dataset',
      '@id': 'temperature',
      'dct:identifier': 'metric:temperature',
      'dct:title': 'temperature',
      'dct:description': 'Temperature sensor',
      'dcat:keyword': ['metric', 'float'],
      'sensor:type': 'float',
      'sensor:seriesCount': 2,
      'sensor:labelDimensions': ['location'],
      'dcat:distribution': [],
    },
  ],
};

const seriesCatalog = {
  '@context': {},
  '@type': 'dcat:Catalog',
  '@id': 'sensapp_series_catalog',
  'dcat:dataset': [
    {
      '@type': 'dcat:Dataset',
      '@id': 'temperature{location="office"}',
      'dct:identifier': 'uuid-office',
      'dct:title': 'temperature',
      'sensor:type': 'float',
      'sensor:labels': [{ location: 'office' }],
      'dcat:distribution': [],
    },
  ],
};

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });

  return function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>{children}</MemoryRouter>
      </QueryClientProvider>
    );
  };
}

describe('App integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockListMetrics.mockResolvedValue({ data: catalogWithMetrics });
    mockListSeries.mockResolvedValue({ data: seriesCatalog });
    mockReadiness.mockResolvedValue({ data: { status: 'ready', database: 'sqlite' } });
    mockGetSeriesData.mockResolvedValue({ data: [] });
    useSelectionStore.setState({
      selectedMetric: null,
      selectedSeries: [],
      labelFilter: '',
    });
  });

  it('renders the header with logo, health badge, and docs link', async () => {
    render(<App />, { wrapper: createWrapper() });

    expect(screen.getByText('SensApp')).toBeInTheDocument();
    expect(await screen.findByText('Connected')).toBeInTheDocument();

    const docsLink = screen.getByText('API Docs').closest('a');
    expect(docsLink).toHaveAttribute('href', '/docs');
    expect(docsLink).toHaveAttribute('target', '_blank');
  });

  it('shows metrics after loading and allows selecting one', async () => {
    const user = userEvent.setup();
    render(<App />, { wrapper: createWrapper() });

    // Metrics should load
    const tempRow = await screen.findByText('temperature');
    expect(tempRow).toBeInTheDocument();

    // Clicking should select the metric
    await user.click(tempRow);
    expect(useSelectionStore.getState().selectedMetric).toBe('temperature');
  });

  it('shows the series panel after a metric is selected', async () => {
    const user = userEvent.setup();
    render(<App />, { wrapper: createWrapper() });

    // Select the metric
    const tempRow = await screen.findByText('temperature');
    await user.click(tempRow);

    // Series panel should show the metric name
    expect(await screen.findByText((_, el) => el?.tagName === 'CODE' && el?.textContent === 'temperature')).toBeInTheDocument();
  });

  it('shows chart placeholder when no series selected', async () => {
    render(<App />, { wrapper: createWrapper() });
    await screen.findByText('temperature'); // wait for load

    expect(screen.getByText(/Select a metric, then choose series to chart/)).toBeInTheDocument();
  });

  it('renders footer', () => {
    render(<App />, { wrapper: createWrapper() });
    expect(screen.getByText(/SINTEF/)).toBeInTheDocument();
  });
});
