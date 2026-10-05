import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import type { ReactNode } from 'react';
import App from './App';
import { createQueryClient } from './api/queryClient';
import { useAuthStore } from './stores/useAuthStore';
import { useSelectionStore } from './stores/useSelectionStore';
import { fakeToken } from './test/fakeToken';

// A server that wants a token: the generated client answers like it does on an HTTP error, with
// the JSON body SensApp sends, {"error": "…"}
const mockListMetrics = vi.fn();
vi.mock('./client', () => ({
  listMetrics: (...args: unknown[]) => mockListMetrics(...args),
  listSeries: vi.fn().mockResolvedValue({ data: { 'dcat:dataset': [] } }),
  getSeriesData: vi.fn().mockResolvedValue({ data: [] }),
}));

const GOOD_TOKEN = fakeToken({ sub: 'alice', scope: 'read', exp: 4_102_444_800 });

const catalog = {
  '@type': 'dcat:Catalog',
  'dcat:dataset': [
    {
      '@type': 'dcat:Dataset',
      '@id': 'temperature',
      'dct:identifier': 'metric:temperature',
      'dct:title': 'temperature',
      'sensor:type': 'float',
      'sensor:seriesCount': 2,
      'dcat:distribution': [],
    },
  ],
};

let calls = 0;

/** Answers the catalog to the good token, and what SensApp answers to anything else. */
function sensappWithJwt(token: string | null) {
  calls++;
  if (token === GOOD_TOKEN) return { data: catalog };
  if (token === 'no-read-scope') {
    return { error: { error: 'Read access required' }, response: { status: 403 } };
  }
  return token
    ? { error: { error: 'Invalid token: ExpiredSignature' }, response: { status: 401 } }
    : { error: { error: 'Missing or invalid Authorization header' }, response: { status: 401 } };
}

function renderApp() {
  const queryClient = createQueryClient();
  // No pause between the asks, and no retry of a failure that is not about authentication
  queryClient.setDefaultOptions({ queries: { staleTime: 30_000, retry: false } });
  const view = render(<App />, {
    wrapper: ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>{children}</MemoryRouter>
      </QueryClientProvider>
    ),
  });
  return { ...view, queryClient };
}

describe('authentication', () => {
  beforeEach(() => {
    calls = 0;
    sessionStorage.clear();
    useAuthStore.setState({ token: null, authRequired: false, dialogOpen: false, message: null });
    useSelectionStore.setState({ selectedMetric: null, selectedSeries: [], labelFilter: '' });
    mockListMetrics.mockImplementation(() => Promise.resolve(sensappWithJwt(useAuthStore.getState().token)));
  });

  it('asks for a token when the server answers 401, then shows the data', async () => {
    const user = userEvent.setup();
    renderApp();

    const dialog = await screen.findByRole('dialog');
    expect(dialog).toHaveTextContent('Authentication required');
    expect(dialog).toHaveTextContent('Missing or invalid Authorization header');
    expect(dialog).toHaveTextContent('sensapp generate-token');
    expect(screen.getByRole('button', { name: 'Use token' })).toBeDisabled();

    await user.type(screen.getByLabelText('JWT'), `Bearer ${GOOD_TOKEN}`);
    await user.click(screen.getByRole('button', { name: 'Use token' }));

    // The refused request is asked again with the token
    expect(await screen.findByText('temperature')).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(useAuthStore.getState().token).toBe(GOOD_TOKEN);
    expect(screen.getByText('alice')).toBeInTheDocument();
  });

  it('does not ask again and again for what the server refuses', async () => {
    renderApp();
    await screen.findByRole('dialog');
    // One request: a refusal is not retried
    expect(calls).toBe(1);
  });

  it('says why a token was not accepted, and asks for another', async () => {
    const user = userEvent.setup();
    renderApp();
    await screen.findByRole('dialog');

    await user.type(screen.getByLabelText('JWT'), 'an.expired.token');
    await user.click(screen.getByRole('button', { name: 'Use token' }));

    const dialog = await screen.findByText('This token is not accepted');
    expect(dialog.closest('[role="dialog"]')).toHaveTextContent('Invalid token: ExpiredSignature');

    await user.type(screen.getByLabelText('JWT'), GOOD_TOKEN);
    await user.click(screen.getByRole('button', { name: 'Use token' }));
    expect(await screen.findByText('temperature')).toBeInTheDocument();
  });

  it('asks for another token when the one it has is not enough (403)', async () => {
    sessionStorage.setItem('sensapp-auth', JSON.stringify({ state: { token: 'no-read-scope' }, version: 0 }));
    await useAuthStore.persist.rehydrate();
    renderApp();

    const dialog = await screen.findByRole('dialog');
    expect(dialog).toHaveTextContent('Read access required');
  });

  it('can be dismissed, and reopened with Sign in', async () => {
    const user = userEvent.setup();
    renderApp();
    await screen.findByRole('dialog');

    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    // The error stays where the data should be
    expect(await screen.findByText(/Failed to load metrics/)).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Sign in' }));
    expect(screen.getByRole('dialog')).toBeInTheDocument();

    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('signs out: forgets the token and the data, and asks again', async () => {
    const user = userEvent.setup();
    useAuthStore.getState().signIn(GOOD_TOKEN);
    const { queryClient } = renderApp();
    expect(await screen.findByText('temperature')).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Sign out' }));

    expect(useAuthStore.getState().token).toBeNull();
    await waitFor(() => expect(screen.getByRole('dialog')).toBeInTheDocument());
    expect(queryClient.getQueryData(['metrics', { name: undefined, type: undefined }])).toBeUndefined();
    expect(screen.queryByText('temperature')).not.toBeInTheDocument();
  });

  it('works without any token when the server does not ask for one', async () => {
    mockListMetrics.mockResolvedValue({ data: catalog });
    renderApp();
    expect(await screen.findByText('temperature')).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Sign in' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Sign out' })).not.toBeInTheDocument();
  });
});
