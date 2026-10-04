import { AGGREGATIONS, FIXED_STEPS } from '../lib/chartStep';
import type { Aggregation } from '../lib/chartStep';
import { PRESETS } from '../lib/timeRange';
import { useSelectionStore } from '../stores/useSelectionStore';

export function TimeRangeSelector() {
  const { timeRange, relativeRange, setTimeRange, setRelativeRange, step, setStep, aggregation, setAggregation } =
    useSelectionStore();

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
    <div className="flex flex-wrap gap-2 items-center">
      <div className="join">
        {PRESETS.map((preset) => (
          <button
            key={preset.label}
            className={`join-item btn btn-xs btn-outline ${relativeRange === preset.label ? 'btn-active' : ''}`}
            onClick={() => setRelativeRange(preset.label)}
            aria-pressed={relativeRange === preset.label}
          >
            {preset.label}
          </button>
        ))}
      </div>

      <div className="flex flex-wrap items-center gap-1 text-xs text-base-content/50">
        <span>from</span>
        <input
          type="datetime-local"
          className="input input-bordered input-xs text-xs h-7"
          value={toLocalDatetime(timeRange.start)}
          onChange={(e) => handleStartChange(e.target.value)}
        />
        <span>to</span>
        <input
          type="datetime-local"
          className="input input-bordered input-xs text-xs h-7"
          value={toLocalDatetime(timeRange.end)}
          onChange={(e) => handleEndChange(e.target.value)}
        />
      </div>

      <div className="flex items-center gap-1">
        <select
          className="select select-bordered select-xs text-xs h-7"
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
          className="select select-bordered select-xs text-xs h-7"
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
