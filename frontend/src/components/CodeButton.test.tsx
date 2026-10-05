import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useAuthStore } from '../stores/useAuthStore';
import { useSelectionStore } from '../stores/useSelectionStore';
import { fakeToken } from '../test/fakeToken';
import { CodeButton } from './CodeButton';

const UUID = '11111111-1111-4111-8111-111111111111';

beforeEach(() => {
  useAuthStore.setState({ token: null, authRequired: false });
  useSelectionStore.setState({
    selectedMetric: 'cpu',
    selector: '',
    selectedSeries: [{ uuid: UUID, name: 'cpu', labels: { host: 'a' }, type: 'float', slot: 0 }],
    timeRange: { start: '2026-10-05T09:00:00.000Z', end: '2026-10-05T10:00:00.000Z' },
    relativeRange: null,
    step: 'raw',
    aggregation: 'avg',
  });
});

afterEach(() => vi.restoreAllMocks());

async function openDialog() {
  const user = userEvent.setup();
  render(<CodeButton />);
  await user.click(screen.getByRole('button', { name: 'Code' }));
  // The dialog is loaded when it is first opened
  await screen.findByRole('dialog', { name: 'Code' });
  return user;
}

const code = () => screen.getByRole('tabpanel').textContent ?? '';

describe('CodeButton', () => {
  it('shows the Python code of the selection, then curl', async () => {
    const user = await openDialog();
    expect(screen.getByRole('tab', { name: 'Python' })).toHaveAttribute('aria-selected', 'true');
    expect(code()).toContain('from sensapp import SensAppClient');
    expect(code()).toContain(UUID);

    await user.click(screen.getByRole('tab', { name: 'curl' }));
    expect(screen.getByRole('tab', { name: 'curl' })).toHaveAttribute('aria-selected', 'true');
    expect(code()).toContain(`curl -sS`);
    expect(code()).toContain(`/series/${UUID}?format=csv`);
  });

  it('colors the code', async () => {
    await openDialog();
    expect(screen.getByRole('tabpanel').querySelectorAll('span.hljs-keyword').length).toBeGreaterThan(0);
    expect(screen.getByRole('tabpanel').querySelector('span.hljs-string')).not.toBeNull();
  });

  it('goes from a tab to the other with the arrow keys, and closes with Escape', async () => {
    const user = await openDialog();
    screen.getByRole('tab', { name: 'Python' }).focus();
    await user.keyboard('{ArrowRight}');
    expect(screen.getByRole('tab', { name: 'curl' })).toHaveFocus();
    expect(screen.getByRole('tab', { name: 'curl' })).toHaveAttribute('aria-selected', 'true');
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('copies the code that is shown', async () => {
    const user = await openDialog();
    await user.click(screen.getByRole('tab', { name: 'curl' }));
    await user.click(screen.getByRole('button', { name: 'Copy' }));
    expect(await navigator.clipboard.readText()).toBe(code());
    expect(screen.getByRole('button', { name: 'Copied' })).toBeInTheDocument();
  });

  it('copies on a page where the clipboard API does not exist (plain http)', async () => {
    const user = await openDialog();
    Object.defineProperty(navigator, 'clipboard', { value: undefined, configurable: true });
    const execCommand = vi.fn().mockReturnValue(true);
    Object.defineProperty(document, 'execCommand', { value: execCommand, configurable: true });
    await user.click(screen.getByRole('button', { name: 'Copy' }));
    expect(execCommand).toHaveBeenCalledWith('copy');
    expect(screen.getByRole('button', { name: 'Copied' })).toBeInTheDocument();
  });

  it('says so when it cannot copy', async () => {
    const user = await openDialog();
    Object.defineProperty(navigator, 'clipboard', { value: undefined, configurable: true });
    Object.defineProperty(document, 'execCommand', { value: vi.fn().mockReturnValue(false), configurable: true });
    await user.click(screen.getByRole('button', { name: 'Copy' }));
    expect(screen.getByRole('button', { name: 'Copy failed' })).toBeInTheDocument();
  });

  it('reads the token from the environment once there is authentication, and never shows it', async () => {
    const token = fakeToken({ sub: 'alice', scope: 'read' });
    useAuthStore.setState({ token });
    const user = await openDialog();
    expect(code()).toContain('os.environ["SENSAPP_TOKEN"]');
    await user.click(screen.getByRole('tab', { name: 'curl' }));
    expect(code()).toContain('Authorization: Bearer $SENSAPP_TOKEN');
    expect(document.body.innerHTML).not.toContain(token);
  });

  it('has no token in the code of a server that asks for none', async () => {
    await openDialog();
    expect(code()).not.toContain('SENSAPP_TOKEN');
  });

  it('follows what is selected: the series of the metric, or the metrics', async () => {
    useSelectionStore.setState({ selectedSeries: [] });
    const first = await openDialog();
    expect(code()).toContain('client.list_series(metric="cpu")');
    expect(screen.getByText(/No series is selected/)).toBeInTheDocument();
    await first.keyboard('{Escape}');

    useSelectionStore.setState({ selectedMetric: null });
    const second = userEvent.setup();
    await second.click(screen.getByRole('button', { name: 'Code' }));
    await screen.findByRole('dialog', { name: 'Code' });
    expect(code()).toContain('client.list_metrics()');
  });

  it('asks for what the selector of the series matches, unless the user prefers the series that are checked', async () => {
    useSelectionStore.setState({ selector: '{host="a"}' });
    const user = await openDialog();
    expect(code()).toContain(`selector='{host="a"}'`);
    expect(code()).not.toContain(UUID);
    const box = screen.getByRole('checkbox', { name: /Read every series matching/ });
    expect(box).toBeChecked();
    expect(screen.getByText(/instead of the 1 selected/)).toBeInTheDocument();

    await user.click(box);
    expect(code()).toContain(UUID);
    expect(code()).not.toContain('selector=');
  });

  it('has no checkbox when no selector is typed', async () => {
    await openDialog();
    expect(screen.queryByRole('checkbox')).toBeNull();
  });
});
