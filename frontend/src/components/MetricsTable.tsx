import { useState } from 'react';
import { useMetrics } from '../hooks/useMetrics';
import type { DcatDataset } from '../hooks/useMetrics';
import { useSelectionStore } from '../stores/useSelectionStore';

const SENSOR_TYPES = [
  '',
  'float',
  'integer',
  'string',
  'boolean',
  'location',
  'json',
  'blob',
  'numeric',
];

export function MetricsTable() {
  const [nameFilter, setNameFilter] = useState('');
  const [typeFilter, setTypeFilter] = useState('');
  const { selectedMetric, setSelectedMetric } = useSelectionStore();

  const { data, isLoading, error } = useMetrics({
    name: nameFilter || undefined,
    type: typeFilter || undefined,
  });

  const metrics = data?.['dcat:dataset'] ?? [];

  function handleSelectMetric(metric: DcatDataset) {
    const metricName = metric['dct:title'];
    if (selectedMetric === metricName) {
      setSelectedMetric(null);
    } else {
      setSelectedMetric(metricName);
    }
  }

  function sensorTypeBadgeClass(type: string): string {
    switch (type) {
      case 'float':
      case 'numeric':
        return 'badge-primary';
      case 'integer':
        return 'badge-secondary';
      case 'string':
        return 'badge-accent';
      case 'boolean':
        return 'badge-info';
      case 'location':
        return 'badge-warning';
      default:
        return 'badge-ghost';
    }
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-3 items-end">
        <div className="form-control flex-1 min-w-48 max-w-xs">
          <label className="label pb-1">
            <span className="label-text text-xs font-medium text-base-content/60">Search metrics</span>
          </label>
          <input
            type="text"
            placeholder="Filter by name..."
            className="input input-bordered input-sm"
            value={nameFilter}
            onChange={(e) => setNameFilter(e.target.value)}
          />
        </div>
        <div className="form-control">
          <label className="label pb-1">
            <span className="label-text text-xs font-medium text-base-content/60">Type</span>
          </label>
          <select
            className="select select-bordered select-sm"
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value)}
          >
            {SENSOR_TYPES.map((t) => (
              <option key={t} value={t}>
                {t || 'All types'}
              </option>
            ))}
          </select>
        </div>
        {!isLoading && !error && (
          <span className="text-xs text-base-content/40 pb-2">
            {metrics.length} metric{metrics.length !== 1 ? 's' : ''}
          </span>
        )}
      </div>

      {isLoading && (
        <div className="flex items-center justify-center gap-2 py-12">
          <span className="loading loading-spinner loading-sm text-primary" />
          <span className="text-sm text-base-content/50">Loading metrics...</span>
        </div>
      )}

      {error && (
        <div className="alert alert-error">
          <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 shrink-0" viewBox="0 0 20 20" fill="currentColor">
            <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
          </svg>
          <span>Failed to load metrics: {error instanceof Error ? error.message : 'Unknown error'}</span>
        </div>
      )}

      {!isLoading && !error && metrics.length === 0 && (
        <div className="text-center py-12">
          <p className="text-sm text-base-content/40">No metrics found</p>
          {(nameFilter || typeFilter) && (
            <button
              className="btn btn-ghost btn-xs mt-2"
              onClick={() => { setNameFilter(''); setTypeFilter(''); }}
            >
              Clear filters
            </button>
          )}
        </div>
      )}

      {!isLoading && !error && metrics.length > 0 && (
        <div className="overflow-x-auto -mx-1">
          <table className="table table-sm w-full">
            <thead>
              <tr className="text-xs text-base-content/50">
                <th className="font-medium">Metric Name</th>
                <th className="font-medium">Type</th>
                <th className="font-medium">Series</th>
                <th className="font-medium hidden sm:table-cell">Dimensions</th>
              </tr>
            </thead>
            <tbody>
              {metrics.map((metric) => {
                const name = metric['dct:title'];
                const isSelected = selectedMetric === name;
                const seriesCount = metric['sensor:seriesCount'];
                const dimensions = metric['sensor:labelDimensions'] ?? [];
                return (
                  <tr
                    key={metric['@id']}
                    className={`cursor-pointer transition-colors ${
                      isSelected
                        ? 'bg-primary/8 border-l-2 border-primary'
                        : 'hover:bg-base-200/60'
                    }`}
                    onClick={() => handleSelectMetric(metric)}
                  >
                    <td>
                      <div className="font-mono text-sm font-medium">{name}</div>
                      {metric['sensor:unit'] && (
                        <div className="text-xs text-base-content/40 mt-0.5">Unit: {metric['sensor:unit']}</div>
                      )}
                    </td>
                    <td>
                      <span className={`badge badge-sm ${sensorTypeBadgeClass(metric['sensor:type'])}`}>
                        {metric['sensor:type']}
                      </span>
                    </td>
                    <td className="tabular-nums text-sm">
                      {seriesCount ?? '—'}
                    </td>
                    <td className="hidden sm:table-cell">
                      <div className="flex flex-wrap gap-1">
                        {dimensions.slice(0, 5).map((dim) => (
                          <span key={dim} className="badge badge-xs badge-outline font-mono">
                            {dim}
                          </span>
                        ))}
                        {dimensions.length === 0 && (
                          <span className="text-xs text-base-content/30">none</span>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
