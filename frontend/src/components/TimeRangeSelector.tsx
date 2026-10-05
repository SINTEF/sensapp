import { AGGREGATIONS, FIXED_STEPS } from '../lib/chartStep';
import type { Aggregation } from '../lib/chartStep';
import { PRESETS } from '../lib/timeRange';
import { useSelectionStore } from '../stores/useSelectionStore';

export function TimeRangeSelector() {
  const {
    timeRange,
    relativeRange,
    rangeHistory,
    setTimeRange,
    setRelativeRange,
    panRange,
    zoomOutRange,
    undoRange,
    step,
    setStep,
    aggregation,
    setAggregation,
  } = useSelectionStore();

  function handleStartChange(value: string) {
    if (value) {
      setTimeRange(new Date(value).toISOString(), timeRange.end);
    }
  }

  function handleEndChange(value: string) {
    if (value) {
      setTimeRange(timeRange.start, new Date(value).toISOString());
    }
  }

  function toLocalDatetime(iso: string): string {
    const d = new Date(iso);
    const offset = d.getTimezoneOffset();
    const local = new Date(d.getTime() - offset * 60 * 1000);
    return local.toISOString().slice(0, 16);
  }

  return (
    <div className="contents">
      {/* Where the window is: back to the one before, earlier, wider, later */}
      <div className="join [&>.btn]:w-7 [&>.btn]:px-0">
        <button
          className="join-item btn btn-xs btn-quiet"
          aria-label="Back to the previous window"
          title="Back to the previous window"
          disabled={rangeHistory.length === 0}
          onClick={undoRange}
        >
          ↶
        </button>
        <button
          className="join-item btn btn-xs btn-quiet"
          aria-label="Earlier"
          title="Half a window earlier"
          onClick={() => panRange(-1)}
        >
          ‹
        </button>
        <button
          className="join-item btn btn-xs btn-quiet"
          aria-label="Zoom out"
          title="Twice the window"
          onClick={zoomOutRange}
        >
          −
        </button>
        <button
          className="join-item btn btn-xs btn-quiet"
          aria-label="Later"
          title="Half a window later"
          // A live window is at now already
          disabled={relativeRange !== null}
          onClick={() => panRange(1)}
        >
          ›
        </button>
      </div>

      <div className="join">
        {PRESETS.map((preset) => (
          <button
            key={preset.label}
            className={`join-item btn btn-xs btn-quiet ${relativeRange === preset.label ? 'btn-active' : ''}`}
            onClick={() => setRelativeRange(preset.label)}
            aria-pressed={relativeRange === preset.label}
          >
            {preset.label}
          </button>
        ))}
      </div>

      <div className="flex flex-wrap items-center gap-x-1 gap-y-1.5 text-xs text-base-content/50">
        <span>from</span>
        <input
          type="datetime-local"
          className="input input-xs text-xs h-7 w-44"
          value={toLocalDatetime(timeRange.start)}
          onChange={(e) => handleStartChange(e.target.value)}
        />
        <span>to</span>
        <input
          type="datetime-local"
          className="input input-xs text-xs h-7 w-44"
          value={toLocalDatetime(timeRange.end)}
          onChange={(e) => handleEndChange(e.target.value)}
        />
      </div>

      <div className="flex items-center gap-1 sm:ml-auto">
        <select
          className="select select-xs text-xs h-7 w-auto"
          aria-label="Step"
          title="Step"
          value={step}
          onChange={(e) => setStep(e.target.value)}
        >
          <option value="auto">Auto</option>
          <option value="raw">Raw</option>
          {FIXED_STEPS.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <select
          className="select select-xs text-xs h-7 w-auto"
          aria-label="Aggregation"
          title="Aggregation"
          value={aggregation}
          disabled={step === 'raw'}
          onChange={(e) => setAggregation(e.target.value as Aggregation)}
        >
          {AGGREGATIONS.map((a) => (
            <option key={a} value={a}>
              {a}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
