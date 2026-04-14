import { MetricsTable } from '../components/MetricsTable';
import { SeriesTable } from '../components/SeriesTable';
import { TimeSeriesChart } from '../components/TimeSeriesChart';
import { TimeRangeSelector } from '../components/TimeRangeSelector';
import { useSelectionStore } from '../stores/useSelectionStore';

export function ExplorerPage() {
  const { selectedMetric, selectedSeries, clearSelectedSeries } =
    useSelectionStore();

  return (
    <div className="flex flex-col gap-3 h-full">
      {/* Chart + Time Range — always on top, à la InfluxDB */}
      <section className="bg-base-100 rounded-lg border border-base-300 shadow-sm shrink-0">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 px-4 py-2 border-b border-base-300">
          <h2 className="text-sm font-semibold">Chart</h2>
          {selectedSeries.length > 0 && (
            <span className="text-xs text-base-content/40">
              {selectedSeries.length} series
            </span>
          )}
          <div className="ml-auto">
            <TimeRangeSelector />
          </div>
        </div>
        <div className="px-2 py-1" style={{ height: '260px' }}>
          {selectedSeries.length > 0 ? (
            <TimeSeriesChart />
          ) : (
            <div className="flex flex-col items-center justify-center h-full text-base-content/30">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-10 w-10 mb-2" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
              </svg>
              <p className="text-sm">
                {selectedMetric
                  ? 'Select series below to visualize data'
                  : 'Select a metric, then choose series to chart'}
              </p>
            </div>
          )}
        </div>
      </section>

      {/* Metrics + Series side-by-side below the chart */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 flex-1 min-h-0">
        {/* Metrics panel */}
        <section className="bg-base-100 rounded-lg border border-base-300 shadow-sm flex flex-col min-h-0">
          <div className="flex items-center justify-between px-4 py-2 border-b border-base-300 shrink-0">
            <h2 className="text-sm font-semibold">Metrics</h2>
            <div className="btn btn-ghost btn-xs invisible" aria-hidden="true" />
          </div>
          <div className="p-3 pt-2 flex-1 min-h-0 flex flex-col">
            <MetricsTable />
          </div>
        </section>

        {/* Series panel */}
        <section className="bg-base-100 rounded-lg border border-base-300 shadow-sm flex flex-col min-h-0">
          <div className="flex items-center justify-between px-4 py-2 border-b border-base-300 shrink-0">
            <h2 className="text-sm font-semibold">
              {selectedMetric ? (
                <>
                  Series{' '}
                  <code className="text-primary bg-primary/5 px-1 py-0.5 rounded text-xs font-mono">{selectedMetric}</code>
                </>
              ) : (
                'Series'
              )}
            </h2>
            <button
              className={`btn btn-ghost btn-xs text-base-content/60 ${selectedSeries.length === 0 ? 'invisible' : ''}`}
              onClick={clearSelectedSeries}
            >
              Clear ({selectedSeries.length})
            </button>
          </div>
          <div className="p-3 pt-2 flex-1 min-h-0 flex flex-col">
            {selectedMetric ? (
              <SeriesTable />
            ) : (
              <div className="text-center py-8 text-base-content/30">
                <p className="text-sm">Select a metric to browse series</p>
              </div>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
