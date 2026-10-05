import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import App from '../App';
import { useAuthStore } from '../stores/useAuthStore';

// The explorer is the other tab: it finds nothing
const emptyCatalog = { '@context': {}, '@type': 'dcat:Catalog', '@id': 'empty', 'dcat:dataset': [] };
vi.mock('../client', () => ({
  listMetrics: () => Promise.resolve({ data: emptyCatalog }),
  listSeries: () => Promise.resolve({ data: emptyCatalog }),
  getSeriesData: () => Promise.resolve({ data: [] }),
}));

beforeEach(() => {
  useAuthStore.setState({ token: null, authRequired: false });
});

function renderAt(path: string) {
  return render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <MemoryRouter initialEntries={[path]}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const code = () => screen.getByRole('tabpanel').textContent ?? '';

describe('LoadPage', () => {
  it('is the second tab of the header, next to the explorer', async () => {
    const user = userEvent.setup();
    renderAt('/');
    const nav = screen.getByRole('navigation', { name: 'Pages' });
    expect(nav).toHaveTextContent('Data Explorer');
    expect(screen.getByRole('link', { name: 'Data Explorer' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('button', { name: 'Code' })).toBeInTheDocument();

    await user.click(screen.getByRole('link', { name: 'Load Data' }));
    expect(await screen.findByRole('tablist', { name: 'Way to load data' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Load Data' })).toHaveAttribute('aria-current', 'page');
    // The Code button is about the explorer
    expect(screen.queryByRole('button', { name: 'Code' })).toBeNull();
  });

  it('opens on the Python SDK, then goes from a way to the other', async () => {
    const user = userEvent.setup();
    renderAt('/load');
    await screen.findByRole('tabpanel');
    expect(screen.getByRole('tab', { name: 'Python SDK' })).toHaveAttribute('aria-selected', 'true');
    expect(code()).toContain('from sensapp import SensAppClient');
    expect(code()).toContain('async with SensAppClient(');

    await user.click(screen.getByRole('tab', { name: 'Telegraf' }));
    expect(code()).toContain('[[outputs.influxdb_v2]]');
    await user.click(screen.getByRole('tab', { name: 'Prometheus' }));
    expect(code()).toContain('remote_write:');
    await user.click(screen.getByRole('tab', { name: 'curl' }));
    expect(code()).toContain('/api/v2/write');
  });

  it('opens on the way the address names, and ignores one it does not know', async () => {
    renderAt('/load?via=prometheus');
    await screen.findByRole('tabpanel');
    expect(screen.getByRole('tab', { name: 'Prometheus' })).toHaveAttribute('aria-selected', 'true');
  });

  it('falls back to the Python SDK for an unknown way', async () => {
    renderAt('/load?via=cobol');
    await screen.findByRole('tabpanel');
    expect(screen.getByRole('tab', { name: 'Python SDK' })).toHaveAttribute('aria-selected', 'true');
  });

  it('goes from a tab to the other with the arrow keys', async () => {
    const user = userEvent.setup();
    renderAt('/load');
    await screen.findByRole('tabpanel');
    screen.getByRole('tab', { name: 'Python SDK' }).focus();
    await user.keyboard('{ArrowRight}');
    expect(screen.getByRole('tab', { name: 'Telegraf' })).toHaveFocus();
    await user.keyboard('{ArrowLeft}{ArrowLeft}');
    expect(screen.getByRole('tab', { name: 'curl' })).toHaveFocus();
  });

  it('colors the code, and copies it', async () => {
    const user = userEvent.setup();
    renderAt('/load?via=curl');
    await screen.findByRole('tabpanel');
    expect(screen.getByRole('tabpanel').querySelector('span.hljs-string')).not.toBeNull();
    await user.click(screen.getByRole('button', { name: 'Copy SenML JSON' }));
    expect(await screen.findByRole('button', { name: 'Copy SenML JSON' })).toHaveTextContent('Copied');
  });

  it('says how to make a token when the server asks for one, and links to where it is made', async () => {
    const user = userEvent.setup();
    useAuthStore.setState({ token: null, authRequired: true });
    renderAt('/load');
    await screen.findByRole('tabpanel');
    expect(screen.getByRole('tabpanel')).toHaveTextContent('sensapp generate-token me --scope write');
    expect(code()).toContain('os.environ["SENSAPP_TOKEN"]');

    // Every way starts with the same section
    for (const way of ['Telegraf', 'Prometheus', 'curl']) {
      await user.click(screen.getByRole('tab', { name: way }));
      expect(screen.getByRole('heading', { name: 'A token' })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: 'Make a token in Credentials' })).toHaveAttribute('href', '/credentials');
    }
  });

  it('says nothing of tokens when the server is open', async () => {
    renderAt('/load');
    await screen.findByRole('tabpanel');
    expect(screen.queryByRole('link', { name: 'Make a token in Credentials' })).toBeNull();
    expect(screen.queryByRole('heading', { name: 'A token' })).toBeNull();
  });
});
