import { CHART_STYLES } from '../lib/chartOption';
import type { ChartStyle } from '../lib/chartOption';
import { useSelectionStore } from '../stores/useSelectionStore';

/** How the series are drawn. */
export function ChartOptions() {
  const { chartStyle, setChartStyle, logScale, setLogScale } = useSelectionStore();

  return (
    <div className="flex items-center gap-1">
      <select
        className="select select-bordered select-xs text-xs h-7"
        aria-label="Style"
        title="Style"
        value={chartStyle}
        onChange={(e) => setChartStyle(e.target.value as ChartStyle)}
      >
        {CHART_STYLES.map((style) => (
          <option key={style} value={style}>
            {style}
          </option>
        ))}
      </select>
      <button
        className={`btn btn-xs btn-outline h-7 ${logScale ? 'btn-active' : ''}`}
        aria-pressed={logScale}
        title="Logarithmic scale"
        onClick={() => setLogScale(!logScale)}
      >
        log
      </button>
    </div>
  );
}
