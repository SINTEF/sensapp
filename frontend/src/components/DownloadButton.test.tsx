import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useAuthStore } from '../stores/useAuthStore';
import { useSelectionStore } from '../stores/useSelectionStore';
import { DownloadButton } from './DownloadButton';

const mockGetSeriesData = vi.fn();
vi.mock('../client', () => ({
  getSeriesData: (...args: unknown[]) => mockGetSeriesData(...args),
}));

const START = '2026-10-05T09:00:00.000Z';
const END = '2026-10-05T10:00:00.000Z';
const kitchen = { uuid: 'uuid-kitchen', name: 'temperature', labels: { room: 'kitchen' }, type: 'float', slot: 0 };
const hall = { uuid: 'uuid-hall', name: 'temperature', labels: { room: 'hall' }, type: 'float', slot: 1 };
const state = { uuid: 'uuid-state', name: 'state', labels: {}, type: 'string', slot: 2 };

/** An answer of the generated client: the file, or the error the server sent */
function file(name: string) {
  return {
    data: new Blob(['timestamp,value\n']),
    response: new Response(null, { headers: { 'Content-Disposition': `attachment; filename="${name}"` } }),
  };
}
function refusal(status: number, message: string) {
  return { error: message, response: { status } };
}

/** The file names given to the links that were clicked */
let saved: string[];

beforeEach(() => {
  vi.clearAllMocks();
  saved = [];
  // jsdom has no object URLs, and does not follow a link
  Object.assign(URL, { createObjectURL: vi.fn(() => 'blob:file'), revokeObjectURL: vi.fn() });
  vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (this: HTMLAnchorElement) {
    saved.push(this.download);
  });
  useAuthStore.setState({ token: null, authRequired: false, dialogOpen: false, message: null });
  useSelectionStore.setState({
    selectedMetric: 'temperature',
    selectedSeries: [kitchen, hall],
    timeRange: { start: START, end: END },
    relativeRange: null,
    step: 'raw',
    aggregation: 'avg',
  });
  mockGetSeriesData.mockImplementation(async ({ path }: { path: { series_uuid: string } }) =>
    file(`${path.series_uuid}.csv`),
  );
});

afterEach(() => vi.restoreAllMocks());

async function openDialog() {
  const user = userEvent.setup();
  render(<DownloadButton />);
  await user.click(screen.getByRole('button', { name: 'Download' }));
  // The dialog is loaded when it is first opened
  await screen.findByRole('dialog', { name: 'Download' });
  return user;
}

function queryOf(uuid: string) {
  return mockGetSeriesData.mock.calls.map(([options]) => options).find((o) => o.path.series_uuid === uuid);
}

describe('DownloadButton', () => {
  it('is off while no series is selected', () => {
    useSelectionStore.setState({ selectedSeries: [] });
    render(<DownloadButton />);
    expect(screen.getByRole('button', { name: 'Download' })).toBeDisabled();
  });

  it('saves one file per series, named by the server', async () => {
    const user = await openDialog();
    await user.click(screen.getByRole('button', { name: 'Download 2 files' }));

    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('2 saved'));
    expect(saved).toEqual(['uuid-kitchen.csv', 'uuid-hall.csv']);
    expect(queryOf('uuid-kitchen')).toMatchObject({
      parseAs: 'blob',
      query: { format: 'csv', download: true, start: START, end: END },
    });
    expect(queryOf('uuid-kitchen').query).not.toHaveProperty('step');
    // The list says what tells the series apart
    const list = within(screen.getByRole('list', { name: 'Series' }));
    expect(list.getByText('temperature{room="kitchen"}')).toBeInTheDocument();
    expect(list.getAllByText('✓ saved').map((status) => status.title)).toEqual(['uuid-kitchen.csv', 'uuid-hall.csv']);
  });

  it('asks the format, every sample, and aggregates numbers only', async () => {
    useSelectionStore.setState({ selectedSeries: [kitchen, state] });
    const user = await openDialog();
    await user.click(screen.getByRole('button', { name: 'Arrow' }));
    await user.click(screen.getByRole('button', { name: 'All data' }));
    await user.click(screen.getByRole('button', { name: 'Aggregated' }));
    await user.selectOptions(screen.getByRole('combobox', { name: 'Step' }), '1d');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Aggregation' }), 'max');
    expect(screen.getByText(/Text and boolean series are downloaded raw/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Download 2 files' }));

    await waitFor(() => expect(mockGetSeriesData).toHaveBeenCalledTimes(2));
    expect(queryOf('uuid-kitchen').query).toEqual({ format: 'arrow', download: true, step: '1d', aggregation: 'max' });
    expect(queryOf('uuid-state').query).toEqual({ format: 'arrow', download: true });
  });

  it('starts from what the chart draws', async () => {
    // 24 hours on `auto`: the chart averages by the minute
    useSelectionStore.setState({ step: 'auto', aggregation: 'min', timeRange: { start: START, end: '2026-10-06T09:00:00.000Z' } });
    await openDialog();
    expect(screen.getByRole('button', { name: 'Aggregated' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('combobox', { name: 'Step' })).toHaveValue('1m');
    expect(screen.getByRole('combobox', { name: 'Aggregation' })).toHaveValue('min');
  });

  it('says why a series was not saved, and saves the others', async () => {
    const message = 'Query exceeds 100000 samples; narrow the time range or use aggregation';
    mockGetSeriesData.mockImplementationOnce(async () => refusal(400, message));
    const user = await openDialog();
    await user.click(screen.getByRole('button', { name: 'Download 2 files' }));

    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('1 saved, 1 failed'));
    expect(screen.getByText(message)).toBeInTheDocument();
    expect(saved).toEqual(['uuid-hall.csv']);
  });

  it('asks for a token when the server refuses it, and stops', async () => {
    mockGetSeriesData.mockImplementationOnce(async () => refusal(401, 'Missing token'));
    const user = await openDialog();
    await user.click(screen.getByRole('button', { name: 'Download 2 files' }));

    await waitFor(() => expect(useAuthStore.getState().dialogOpen).toBe(true));
    expect(useAuthStore.getState().message).toBe('Missing token');
    expect(mockGetSeriesData).toHaveBeenCalledTimes(1);
    expect(saved).toEqual([]);
  });

  it('stops the download when the dialog is closed', async () => {
    let signal: AbortSignal | undefined;
    mockGetSeriesData.mockImplementation((options: { signal: AbortSignal }) => {
      signal = options.signal;
      return new Promise(() => {});
    });
    const user = await openDialog();
    await user.click(screen.getByRole('button', { name: 'Download 2 files' }));
    expect(screen.getByRole('button', { name: 'Downloading…' })).toHaveAttribute('aria-disabled', 'true');
    // A second click does not start the downloads again
    await user.click(screen.getByRole('button', { name: 'Downloading…' }));
    expect(mockGetSeriesData).toHaveBeenCalledTimes(1);

    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(signal?.aborted).toBe(true);
    expect(mockGetSeriesData).toHaveBeenCalledTimes(1);
  });
});
