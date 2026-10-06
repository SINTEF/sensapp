import { useEffect, useRef, useState } from 'react';
import { getSeriesData } from '../client';
import { ApiError, unwrap } from '../api/clientConfig';
import { AGGREGATIONS, ROUND_STEPS, isNumericType, resolveStep } from '../lib/chartStep';
import type { Aggregation } from '../lib/chartStep';
import { DOWNLOAD_FORMATS, downloadQuery, fallbackFileName, fileNameFrom, saveBlob } from '../lib/download';
import type { DownloadChoice } from '../lib/download';
import { seriesLabel, sharedLabels, withoutShared } from '../lib/seriesLabels';
import { useAuthStore } from '../stores/useAuthStore';
import { useSelectionStore } from '../stores/useSelectionStore';

type Status =
  | { state: 'waiting' }
  | { state: 'downloading' }
  | { state: 'saved'; file: string }
  | { state: 'failed'; message: string };

/** The step offered when the chart has none */
const DEFAULT_STEP = '1h';

/** Buttons of which one is on, like the presets of the chart. */
function Choice<T extends string>({
  label,
  options,
  value,
  disabled,
  onChange,
}: {
  label: string;
  options: ReadonlyArray<{ id: T; label: string }>;
  value: T;
  disabled: boolean;
  onChange: (value: T) => void;
}) {
  return (
    <div role="group" aria-label={label} className="join [&>.btn]:h-7">
      {options.map((option) => (
        <button
          key={option.id}
          type="button"
          className="join-item btn btn-xs btn-quiet"
          aria-pressed={value === option.id}
          disabled={disabled}
          onClick={() => onChange(option.id)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

function StatusText({ status }: { status: Status | undefined }) {
  switch (status?.state) {
    case undefined:
      return null;
    case 'waiting':
      return <span className="text-base-content/40">waiting</span>;
    case 'downloading':
      return <span className="loading loading-spinner loading-xs text-primary" aria-label="downloading" />;
    case 'saved':
      // The name is long (labels, window, step): the series says it better
      return (
        <span className="text-success whitespace-nowrap" title={status.file}>
          ✓ saved
        </span>
      );
    case 'failed':
      return <span className="text-error">{status.message}</span>;
  }
}

/** The selected series as files: one file per series, saved one after the other. */
export default function DownloadDialog({ onClose }: { onClose: () => void }) {
  const { selectedSeries, timeRange, step, aggregation } = useSelectionStore();

  // What the chart draws is what is offered first
  const [chartStep] = useState(() => resolveStep(step, timeRange.start, timeRange.end));
  const [steps] = useState(() => (!chartStep || ROUND_STEPS.includes(chartStep) ? ROUND_STEPS : [chartStep, ...ROUND_STEPS]));
  const [choice, setChoice] = useState<DownloadChoice>(() => ({
    format: 'csv',
    window: 'chart',
    sampling: chartStep ? 'aggregated' : 'raw',
    step: chartStep ?? DEFAULT_STEP,
    aggregation,
  }));
  const change = (part: Partial<DownloadChoice>) => setChoice((current) => ({ ...current, ...part }));

  const [statuses, setStatuses] = useState<Record<string, Status>>({});
  const [running, setRunning] = useState(false);
  const controller = useRef<AbortController>(undefined);
  // Closing the dialog stops what is left
  useEffect(() => () => controller.current?.abort(), []);

  const shared = sharedLabels(selectedSeries.map((s) => s.labels));
  const notNumeric = selectedSeries.filter((s) => !isNumericType(s.type)).length;

  async function download() {
    const current = new AbortController();
    controller.current = current;
    setRunning(true);
    setStatuses(Object.fromEntries(selectedSeries.map((s) => [s.uuid, { state: 'waiting' }])));
    const set = (uuid: string, status: Status) => setStatuses((all) => ({ ...all, [uuid]: status }));

    for (const series of selectedSeries) {
      set(series.uuid, { state: 'downloading' });
      try {
        const result = await getSeriesData({
          path: { series_uuid: series.uuid },
          query: downloadQuery(series, choice, timeRange),
          parseAs: 'blob',
          signal: current.signal,
        });
        const blob = unwrap(result) as unknown as Blob;
        const file =
          fileNameFrom(result.response?.headers.get('Content-Disposition')) ?? fallbackFileName(series, choice.format);
        saveBlob(blob, file);
        set(series.uuid, { state: 'saved', file });
      } catch (error) {
        if (current.signal.aborted) return;
        const message = error instanceof Error ? error.message : 'Unknown error';
        set(series.uuid, { state: 'failed', message });
        // Asking again does not make a token valid: the sign-in dialog, then Download again
        if (error instanceof ApiError && error.isAuthError) {
          useAuthStore.getState().reject(message);
          break;
        }
      }
    }
    setRunning(false);
  }

  const values = Object.values(statuses);
  const saved = values.filter((s) => s.state === 'saved').length;
  const failed = values.filter((s) => s.state === 'failed').length;
  const count = selectedSeries.length;

  return (
    <div
      className="modal modal-open"
      role="dialog"
      aria-modal="true"
      aria-labelledby="download-dialog-title"
      onKeyDown={(event) => event.key === 'Escape' && onClose()}
    >
      <div className="modal-box max-w-xl p-0 overflow-hidden flex flex-col max-h-[90vh]">
        <div className="flex items-center justify-between gap-3 px-4 py-2 border-b border-base-300">
          <h2 id="download-dialog-title" className="text-sm font-semibold">
            Download
          </h2>
          <button type="button" className="btn btn-ghost btn-xs btn-circle" aria-label="Close" onClick={onClose}>
            ✕
          </button>
        </div>

        <div className="grid grid-cols-[auto_1fr] items-center gap-x-3 gap-y-2 px-4 pt-3 text-xs">
          <span className="text-base-content/60">Format</span>
          <Choice
            label="Format"
            options={DOWNLOAD_FORMATS}
            value={choice.format}
            disabled={running}
            onChange={(format) => change({ format })}
          />

          <span className="text-base-content/60">Window</span>
          <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
            <Choice
              label="Window"
              options={[
                { id: 'chart', label: 'Chart window' },
                { id: 'all', label: 'All data' },
              ]}
              value={choice.window}
              disabled={running}
              onChange={(window) => change({ window })}
            />
            {choice.window === 'chart' && (
              <span className="text-base-content/50">
                {new Date(timeRange.start).toLocaleString()} – {new Date(timeRange.end).toLocaleString()}
              </span>
            )}
          </div>

          <span className="text-base-content/60">Samples</span>
          <div className="flex flex-wrap items-center gap-1">
            <Choice
              label="Samples"
              options={[
                { id: 'raw', label: 'Raw' },
                { id: 'aggregated', label: 'Aggregated' },
              ]}
              value={choice.sampling}
              disabled={running}
              onChange={(sampling) => change({ sampling })}
            />
            {choice.sampling === 'aggregated' && (
              <>
                <select
                  className="select select-xs text-xs h-7 w-auto"
                  aria-label="Step"
                  title="Step"
                  value={choice.step}
                  disabled={running}
                  onChange={(event) => change({ step: event.target.value })}
                >
                  {steps.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
                <select
                  className="select select-xs text-xs h-7 w-auto"
                  aria-label="Aggregation"
                  title="Aggregation"
                  value={choice.aggregation}
                  disabled={running}
                  onChange={(event) => change({ aggregation: event.target.value as Aggregation })}
                >
                  {AGGREGATIONS.map((a) => (
                    <option key={a} value={a}>
                      {a}
                    </option>
                  ))}
                </select>
              </>
            )}
          </div>
        </div>

        <p className="px-4 pt-2 text-xs text-base-content/50">
          One file per series. A series over the sample limit of the server is not saved: narrow the window or
          aggregate it.
          {choice.sampling === 'aggregated' && notNumeric > 0 && ' Text and boolean series are downloaded raw.'}
        </p>

        <ul aria-label="Series" className="mx-4 mt-2 min-h-0 overflow-y-auto border border-base-300 rounded-lg divide-y divide-base-300 text-xs">
          {selectedSeries.map((series) => (
            <li key={series.uuid} className="flex items-baseline justify-between gap-3 px-3 py-1.5">
              <span className="font-mono truncate min-w-0 shrink" title={seriesLabel(series.name, series.labels)}>
                {seriesLabel(series.name, withoutShared(series.labels, shared))}
              </span>
              <span className="flex min-w-0 max-w-[60%] justify-end text-right">
                <StatusText status={statuses[series.uuid]} />
              </span>
            </li>
          ))}
        </ul>

        <div className="flex items-center justify-end gap-3 px-4 py-3">
          <span role="status" className="text-xs text-base-content/60">
            {!running && values.length > 0 && `${saved} saved${failed > 0 ? `, ${failed} failed` : ''}`}
          </span>
          {/* Not `disabled`: the button would lose the focus, and the dialog Escape with it */}
          <button
            type="button"
            className="btn btn-primary btn-sm"
            autoFocus
            aria-disabled={running}
            onClick={() => !running && void download()}
          >
            {running ? 'Downloading…' : `Download ${count} ${count === 1 ? 'file' : 'files'}`}
          </button>
        </div>
      </div>
      <button type="button" className="modal-backdrop" aria-label="Close" onClick={onClose} />
    </div>
  );
}
