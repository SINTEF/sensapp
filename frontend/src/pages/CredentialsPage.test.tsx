import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import App from '../App';
import { createQueryClient } from '../api/queryClient';
import { useAuthStore } from '../stores/useAuthStore';
import { fakeToken } from '../test/fakeToken';

const mockListMetrics = vi.fn();
const mockCreateToken = vi.fn();
vi.mock('../client', () => ({
  listMetrics: (...args: unknown[]) => mockListMetrics(...args),
  listSeries: () => Promise.resolve({ data: { 'dcat:dataset': [] } }),
  getSeriesData: () => Promise.resolve({ data: [] }),
  createToken: (...args: unknown[]) => mockCreateToken(...args),
}));

const ADMIN = fakeToken({ sub: 'root', scope: 'admin', exp: 4_102_444_800 });
const READER = fakeToken({ sub: 'reader', scope: 'read', exp: 4_102_444_800 });
// The token the local run prints has every scope
const LOCAL = fakeToken({ sub: 'local-dev', scope: 'read write delete admin', exp: 4_102_444_800 });

const catalog = {
  'dcat:dataset': [
    { '@id': 'a', 'dct:title': 'temperature' },
    { '@id': 'b', 'dct:title': 'humidity' },
  ],
};

const created = {
  token: 'made.by.sensapp',
  subject: 'edge-7',
  scope: ['read', 'write'],
  sensors: null,
  expires_at: 4_102_444_800,
  jti: '6f9c2f5e-0000-4000-8000-000000000000',
};

function renderPage() {
  const queryClient = createQueryClient();
  queryClient.setDefaultOptions({ queries: { retry: false, staleTime: 30_000 } });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/credentials']}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const command = () => screen.getByLabelText('Command that makes the token').textContent ?? '';

beforeEach(() => {
  sessionStorage.clear();
  useAuthStore.setState({ token: null, authRequired: false, dialogOpen: false, message: null });
  mockListMetrics.mockReset();
  mockCreateToken.mockReset();
  mockListMetrics.mockResolvedValue({ data: catalog });
});

describe('CredentialsPage', () => {
  it('is a tab of the header', async () => {
    renderPage();
    expect(await screen.findByRole('link', { name: 'Credentials' })).toHaveAttribute('aria-current', 'page');
  });

  it('says that nothing needs a token when the server is open', async () => {
    renderPage();
    expect(await screen.findByText('Authentication is disabled')).toBeInTheDocument();
    expect(screen.queryByRole('textbox', { name: 'Name' })).toBeNull();
  });

  it('asks for the choices in the order of their weight: the validity before the scopes', async () => {
    useAuthStore.setState({ token: ADMIN });
    renderPage();
    await screen.findByRole('textbox', { name: 'Name' });
    const titles = screen.getAllByRole('heading', { level: 2 }).map((heading) => heading.textContent);
    expect(titles).toEqual(['Name', 'Valid for', 'What it may do', 'Only these sensors', 'Command line', 'Make it here']);
  });

  it('shows the form and the command, and how to get an admin token, to a server that wants a token', async () => {
    mockListMetrics.mockResolvedValue({ error: { error: 'Missing or invalid Authorization header' }, response: { status: 401 } });
    renderPage();
    expect(await screen.findByRole('textbox', { name: 'Name' })).toBeInTheDocument();
    expect(command()).toContain('sensapp generate-token NAME --scope read,write --duration 86400');
    expect(screen.getByLabelText('Command that makes an admin token')).toHaveTextContent('sensapp generate-token me --scope admin');
    expect(screen.queryByRole('button', { name: 'Make the token' })).toBeNull();
    // The sign-in dialog opened with the answer of the server, and the command of an admin token
    const dialog = await screen.findByRole('dialog');
    expect(dialog).toHaveTextContent('sensapp generate-token me --scope admin');
    expect(dialog).not.toHaveTextContent('--scope read');
  });

  it('says that the token in use is not an admin one, and leaves the command', async () => {
    useAuthStore.setState({ token: READER });
    renderPage();
    expect(await screen.findByText(/does not have it/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Make the token' })).toBeNull();
    expect(command()).toContain('sensapp generate-token NAME');
  });

  it('does not read the catalog with an admin token, which cannot read: the refusal would ask for a token again', async () => {
    useAuthStore.setState({ token: ADMIN });
    renderPage();
    expect(await screen.findByRole('button', { name: 'Make the token' })).toBeInTheDocument();
    expect(mockListMetrics).not.toHaveBeenCalled();
    expect(useAuthStore.getState().dialogOpen).toBe(false);
  });

  it('writes the command as the form is filled, quoted, with a --sensor for each name', async () => {
    const user = userEvent.setup();
    useAuthStore.setState({ token: ADMIN });
    renderPage();

    await user.type(await screen.findByRole('textbox', { name: 'Name' }), "bob's phone");
    await user.click(screen.getByRole('radio', { name: '1 hour' }));
    await user.click(screen.getByRole('checkbox', { name: /^delete/ }));
    const sensor = screen.getByRole('textbox', { name: 'Sensor name' });
    await user.type(sensor, 'cpu,usage{Enter}');
    await user.type(sensor, 'temperature{Enter}');

    expect(command()).toBe(
      "sensapp generate-token 'bob'\\''s phone' --scope read,write,delete --duration 3600 \\\n  --sensor 'cpu,usage' \\\n  --sensor temperature\n",
    );
  });

  it('makes an admin token with the command only', async () => {
    const user = userEvent.setup();
    useAuthStore.setState({ token: ADMIN });
    renderPage();

    await user.type(await screen.findByRole('textbox', { name: 'Name' }), 'root');
    await user.click(screen.getByRole('checkbox', { name: /^admin/ }));
    expect(command()).toContain('--scope read,write,admin');
    expect(screen.getByText(/only made with the command line/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Make the token' })).toBeDisabled();
    expect(mockCreateToken).not.toHaveBeenCalled();
  });

  it('makes a token with what the form says, shows it once, then lets it go', async () => {
    const user = userEvent.setup();
    useAuthStore.setState({ token: ADMIN });
    mockCreateToken.mockResolvedValue({ data: created });
    renderPage();

    const submit = await screen.findByRole('button', { name: 'Make the token' });
    // A name is needed, and at least one scope
    expect(submit).toBeDisabled();
    await user.type(screen.getByRole('textbox', { name: 'Name' }), '  edge-7 ');
    expect(submit).toBeEnabled();
    await user.click(screen.getByRole('radio', { name: '1 hour' }));
    const sensor = screen.getByRole('textbox', { name: 'Sensor name' });
    await user.type(sensor, 'temperature{Enter}');
    await user.type(sensor, 'cpu,usage{Enter}');
    await user.type(sensor, 'temperature{Enter}');
    await user.click(submit);

    // A comma is part of a name, and the repeated name is kept once
    expect(mockCreateToken).toHaveBeenCalledWith({
      body: { subject: 'edge-7', scope: ['read', 'write'], sensors: ['temperature', 'cpu,usage'], duration_seconds: 3600 },
    });

    expect(await screen.findByText(/only time it is shown/)).toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: 'Token' })).toHaveValue('made.by.sensapp');
    expect(screen.getByLabelText('Export of the token')).toHaveTextContent('export SENSAPP_TOKEN=made.by.sensapp');
    expect(screen.getByText(created.jti)).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Copy' }));
    expect(await screen.findByRole('button', { name: 'Copied' })).toBeInTheDocument();
    expect(await navigator.clipboard.readText()).toBe('made.by.sensapp');

    await user.click(screen.getByRole('button', { name: 'Done' }));
    await waitFor(() => expect(screen.queryByRole('textbox', { name: 'Token' })).toBeNull());
    // The form is still there, with what was typed
    expect(screen.getByRole('textbox', { name: 'Name' })).toHaveValue('  edge-7 ');
  });

  it('lets a sensor go, and does not add a blank one', async () => {
    const user = userEvent.setup();
    useAuthStore.setState({ token: ADMIN });
    mockCreateToken.mockResolvedValue({ data: created });
    renderPage();

    const sensor = await screen.findByRole('textbox', { name: 'Sensor name' });
    expect(screen.getByRole('button', { name: 'Add' })).toBeDisabled();
    await user.type(sensor, '   {Enter}');
    expect(screen.queryByRole('list', { name: 'Allowed sensors' })).toBeNull();

    await user.type(sensor, 'temperature{Enter}humidity{Enter}');
    const chips = screen.getByRole('list', { name: 'Allowed sensors' });
    expect(within(chips).getAllByRole('listitem')).toHaveLength(2);
    await user.click(screen.getByRole('button', { name: 'Remove temperature' }));
    expect(within(screen.getByRole('list', { name: 'Allowed sensors' })).getAllByRole('listitem')).toHaveLength(1);
    expect(command()).toContain('--sensor humidity');
    expect(command()).not.toContain('temperature');
  });

  it('offers the sensors of the server to a token that can read, to click in', async () => {
    const user = userEvent.setup();
    useAuthStore.setState({ token: LOCAL });
    renderPage();

    await user.click(await screen.findByRole('button', { name: 'temperature' }));
    await user.click(screen.getByRole('button', { name: 'humidity' }));
    expect(within(screen.getByRole('list', { name: 'Allowed sensors' })).getAllByRole('listitem')).toHaveLength(2);
    // What is chosen is not offered again
    expect(screen.queryByRole('button', { name: 'temperature' })).toBeNull();
  });

  it('shows what the server refused in the form, to fix it', async () => {
    const user = userEvent.setup();
    useAuthStore.setState({ token: ADMIN });
    mockCreateToken.mockResolvedValue({
      error: { error: 'The duration cannot be longer than 3600 seconds' },
      response: { status: 400 },
    });
    renderPage();

    await user.type(await screen.findByRole('textbox', { name: 'Name' }), 'edge-7');
    await user.click(screen.getByRole('button', { name: 'Make the token' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('The duration cannot be longer than 3600 seconds');
    expect(screen.getByRole('textbox', { name: 'Name' })).toHaveValue('edge-7');
    expect(useAuthStore.getState().dialogOpen).toBe(false);
  });

  it('asks again for a token when the server says the one in use is not an admin one', async () => {
    const user = userEvent.setup();
    useAuthStore.setState({ token: ADMIN });
    mockCreateToken.mockResolvedValue({ error: { error: 'admin access required' }, response: { status: 403 } });
    renderPage();

    await user.type(await screen.findByRole('textbox', { name: 'Name' }), 'edge-7');
    await user.click(screen.getByRole('button', { name: 'Make the token' }));

    await waitFor(() => expect(useAuthStore.getState().dialogOpen).toBe(true));
    expect(useAuthStore.getState().message).toBe('admin access required');
    // The refusal is for the dialog, not repeated in the form
    expect(within(screen.getByRole('main')).queryByRole('alert')).toBeNull();
  });
});
