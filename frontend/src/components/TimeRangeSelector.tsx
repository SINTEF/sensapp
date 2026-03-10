import { useSelectionStore } from '../stores/useSelectionStore';

const PRESETS = [
  { label: '15m', minutes: 15 },
  { label: '1h', minutes: 60 },
  { label: '6h', minutes: 360 },
  { label: '24h', minutes: 1440 },
  { label: '7d', minutes: 10080 },
  { label: '30d', minutes: 43200 },
];

export function TimeRangeSelector() {
  const { timeRange, setTimeRange } = useSelectionStore();

  function handlePreset(minutes: number) {
    const end = new Date();
    const start = new Date(end.getTime() - minutes * 60 * 1000);
    setTimeRange(start.toISOString(), end.toISOString());
  }

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
    <div className="flex flex-wrap gap-3 items-end">
      <div>
        <span className="text-xs font-medium text-base-content/60 block mb-1">Presets</span>
        <div className="join">
          {PRESETS.map((preset) => (
            <button
              key={preset.label}
              className="join-item btn btn-sm btn-outline"
              onClick={() => handlePreset(preset.minutes)}
            >
              {preset.label}
            </button>
          ))}
        </div>
      </div>

      <div className="form-control">
        <label className="label pb-1">
          <span className="label-text text-xs font-medium text-base-content/60">From</span>
        </label>
        <input
          type="datetime-local"
          className="input input-bordered input-sm text-xs"
          value={toLocalDatetime(timeRange.start)}
          onChange={(e) => handleStartChange(e.target.value)}
        />
      </div>

      <div className="form-control">
        <label className="label pb-1">
          <span className="label-text text-xs font-medium text-base-content/60">To</span>
        </label>
        <input
          type="datetime-local"
          className="input input-bordered input-sm text-xs"
          value={toLocalDatetime(timeRange.end)}
          onChange={(e) => handleEndChange(e.target.value)}
        />
      </div>
    </div>
  );
}
