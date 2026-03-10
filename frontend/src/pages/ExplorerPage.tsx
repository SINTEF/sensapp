import { MetricsTable } from '../components/MetricsTable';
import { SeriesTable } from '../components/SeriesTable';
import { TimeSeriesChart } from '../components/TimeSeriesChart';
import { TimeRangeSelector } from '../components/TimeRangeSelector';
import { useSelectionStore } from '../stores/useSelectionStore';

export function ExplorerPage() {
  const { selectedMetric, selectedSeries, clearSelectedSeries } =
    useSelectionStore();

  return (
    <div className="space-y-6">
      {/* Metrics Section */}
      <section>
        <div className="flex items-center gap-2 mb-3">
          <h2 className="text-base font-semibold">Metrics</h2>
          <span className="text-xs text-base-content/40">Browse available measurement types</span>
        </div>
        <div className="bg-base-100 rounded-xl border border-base-300 shadow-sm p-5">
          <MetricsTable />
        </div>
      </section>

      {/* Series Section */}
      {selectedMetric && (
        <section>
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <h2 className="text-base font-semibold">
                Series for{' '}
                <code className="text-primary bg-primary/5 px-1.5 py-0.5 rounded text-sm">{selectedMetric}</code>
              </h2>
            </div>
            {selectedSeries.length > 0 && (
              <button
                className="btn btn-ghost btn-xs text-base-content/60"
                onClick={clearSelectedSeries}
              >
                Clear selection ({selectedSeries.length})
              </button>
            )}
          </div>
          <div className="bg-base-100 rounded-xl border border-base-300 shadow-sm p-5">
            <SeriesTable />
          </div>
        </section>
      )}

      {/* Chart Section */}
      {selectedSeries.length > 0 && (
        <section>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-base font-semibold">Chart</h2>
          </div>
          <div className="bg-base-100 rounded-xl border border-base-300 shadow-sm p-5">
            <TimeRangeSelector />
            <div className="mt-4 -mx-2">
              <TimeSeriesChart />
            </div>
          </div>
        </section>
      )}

      {/* Empty state when nothing is selected */}
      {!selectedMetric && (
        <div className="text-center py-12">
          <svg xmlns="http://www.w3.org/2000/svg" className="h-12 w-12 mx-auto mb-3 text-base-content/20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
          </svg>
          <p className="text-sm text-base-content/40">
            Select a metric above to explore its time series data
          </p>
        </div>
      )}
    </div>
  );
}
