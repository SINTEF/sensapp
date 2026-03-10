import { useState } from 'react';
import { useSeries } from '../hooks/useSeries';
import type { SeriesDataset } from '../hooks/useSeries';
import { useSelectionStore } from '../stores/useSelectionStore';

function labelsToRecord(
  labels?: Array<Record<string, string>>
): Record<string, string> {
  if (!labels || labels.length === 0) return {};
  return labels.reduce<Record<string, string>>(
    (acc, l) => ({ ...acc, ...l }),
    {}
  );
}

export function SeriesTable() {
  const {
    selectedMetric,
    selectedSeries,
    toggleSeries,
    labelFilter,
    setLabelFilter,
  } = useSelectionStore();
  const [selectorInput, setSelectorInput] = useState('');

  const selector = selectorInput || undefined;

  const { data, isLoading, error } = useSeries({
    metric: selectedMetric || undefined,
    selector,
  });

  const series = data?.['dcat:dataset'] ?? [];

  function isSelected(dataset: SeriesDataset): boolean {
    return selectedSeries.some(
      (s) => s.uuid === dataset['dct:identifier']
    );
  }

  function handleToggle(dataset: SeriesDataset) {
    toggleSeries({
      uuid: dataset['dct:identifier'],
      name: dataset['dct:title'],
      labels: labelsToRecord(dataset['sensor:labels']),
      type: dataset['sensor:type'],
    });
  }

  // Client-side label text filtering
  const filteredSeries = labelFilter
    ? series.filter((s) => {
        const labelPairs = (s['sensor:labels'] ?? [])
          .flatMap((obj) => Object.entries(obj))
          .map(([k, v]) => `${k}=${v}`)
          .join(' ');
        const searchStr = `${s['dct:title']} ${labelPairs}`.toLowerCase();
        return searchStr.includes(labelFilter.toLowerCase());
      })
    : series;

  if (!selectedMetric) {
    return null;
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-3 items-end">
        <div className="form-control flex-1 min-w-56 max-w-sm">
          <label className="label pb-1">
            <span className="label-text text-xs font-medium text-base-content/60">
              PromQL Label Selector
            </span>
          </label>
          <input
            type="text"
            placeholder='{env="prod", region=~"us.*"}'
            className="input input-bordered input-sm font-mono text-xs"
            value={selectorInput}
            onChange={(e) => setSelectorInput(e.target.value)}
          />
        </div>
        <div className="form-control min-w-36 max-w-48">
          <label className="label pb-1">
            <span className="label-text text-xs font-medium text-base-content/60">Quick filter</span>
          </label>
          <input
            type="text"
            placeholder="Filter labels..."
            className="input input-bordered input-sm"
            value={labelFilter}
            onChange={(e) => setLabelFilter(e.target.value)}
          />
        </div>
        {selectedSeries.length > 0 && (
          <span className="text-xs text-primary font-medium pb-2">
            {selectedSeries.length} selected
          </span>
        )}
      </div>

      {isLoading && (
        <div className="flex items-center justify-center gap-2 py-12">
          <span className="loading loading-spinner loading-sm text-primary" />
          <span className="text-sm text-base-content/50">Loading series...</span>
        </div>
      )}

      {error && (
        <div className="alert alert-error">
          <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 shrink-0" viewBox="0 0 20 20" fill="currentColor">
            <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
          </svg>
          <span>Failed to load series: {error instanceof Error ? error.message : 'Unknown error'}</span>
        </div>
      )}

      {!isLoading && !error && filteredSeries.length === 0 && (
        <div className="text-center py-8">
          <p className="text-sm text-base-content/40">
            {series.length === 0 ? 'No series found for this metric' : 'No series match the current filter'}
          </p>
        </div>
      )}

      {!isLoading && !error && filteredSeries.length > 0 && (
        <div className="overflow-x-auto -mx-1">
          <table className="table table-sm w-full">
            <thead>
              <tr className="text-xs text-base-content/50">
                <th className="w-8 font-medium">
                  <span className="sr-only">Select</span>
                </th>
                <th className="font-medium">Series ID</th>
                <th className="font-medium">Type</th>
                <th className="font-medium">Labels</th>
              </tr>
            </thead>
            <tbody>
              {filteredSeries.map((s) => {
                const checked = isSelected(s);
                return (
                  <tr
                    key={s['dct:identifier']}
                    className={`cursor-pointer transition-colors ${
                      checked
                        ? 'bg-primary/8'
                        : 'hover:bg-base-200/60'
                    }`}
                    onClick={() => handleToggle(s)}
                  >
                    <td>
                      <input
                        type="checkbox"
                        className="checkbox checkbox-primary checkbox-xs"
                        checked={checked}
                        onChange={() => handleToggle(s)}
                        onClick={(e) => e.stopPropagation()}
                      />
                    </td>
                    <td className="font-mono text-xs text-base-content/60 max-w-[10rem] truncate">
                      {s['dct:identifier']}
                    </td>
                    <td>
                      <span className="badge badge-xs badge-outline">
                        {s['sensor:type']}
                      </span>
                    </td>
                    <td>
                      <div className="flex flex-wrap gap-1">
                        {(s['sensor:labels'] ?? []).map((labelObj, i) =>
                          Object.entries(labelObj).map(([k, v]) => (
                            <span
                              key={`${i}-${k}`}
                              className="inline-flex items-center gap-0.5 text-xs bg-base-200 rounded px-1.5 py-0.5"
                            >
                              <span className="text-base-content/50">{k}=</span>
                              <span className="font-medium">{v}</span>
                            </span>
                          ))
                        )}
                        {(!s['sensor:labels'] || s['sensor:labels'].length === 0) && (
                          <span className="text-xs text-base-content/30">no labels</span>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <div className="text-xs text-base-content/40 mt-2">
            {filteredSeries.length} series
            {filteredSeries.length !== series.length && ` (${series.length} total)`}
          </div>
        </div>
      )}
    </div>
  );
}
