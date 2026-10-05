import { useEffect, useRef, useState } from 'react';
import { useMetrics } from '../hooks/useMetrics';
import type { DcatDataset } from '../hooks/useMetrics';
import { useSelectionStore } from '../stores/useSelectionStore';
import { ErrorAlert, Loading } from './Feedback';
import { Panel } from './Panel';

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

  // The page opened on a metric (the address says which): the list starts there, once. A click is on
  // a row that is on screen, the list does not move for it.
  const list = useRef<HTMLDivElement>(null);
  const placed = useRef(false);
  useEffect(() => {
    if (placed.current || !selectedMetric || !list.current) return;
    const row = list.current.querySelector<HTMLElement>('[data-selected="true"]');
    if (!row) return;
    placed.current = true;
    const box = list.current.getBoundingClientRect();
    const rect = row.getBoundingClientRect();
    list.current.scrollTop += rect.top - box.top - (box.height - rect.height) / 2;
  }, [data, selectedMetric]);

  function handleSelectMetric(metric: DcatDataset) {
    placed.current = true;
    const metricName = metric['dct:title'];
    if (selectedMetric === metricName) {
      setSelectedMetric(null);
    } else {
      setSelectedMetric(metricName);
    }
  }

  // The server says `Float`, the filter above `float`
  function sensorTypeBadgeClass(type: string): string {
    switch (type.toLowerCase()) {
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

  const controls = (
    <>
      <input
        type="text"
        placeholder="Filter by name..."
        aria-label="Filter by name"
        className="input input-xs text-xs w-40 h-7"
        value={nameFilter}
        onChange={(e) => setNameFilter(e.target.value)}
      />
      <select
        className="select select-xs text-xs h-7 w-auto"
        aria-label="Filter by type"
        value={typeFilter}
        onChange={(e) => setTypeFilter(e.target.value)}
      >
        {SENSOR_TYPES.map((t) => (
          <option key={t} value={t}>
            {t || 'All types'}
          </option>
        ))}
      </select>
      {!isLoading && !error && (
        <span className="text-xs text-base-content/40">
          {metrics.length} metric{metrics.length !== 1 ? 's' : ''}
        </span>
      )}
    </>
  );

  return (
    <Panel title="Metrics" controls={controls}>
      <div ref={list} className="flex-1 min-h-0 overflow-y-auto overflow-x-auto">
      {isLoading && <Loading />}

      {error && <ErrorAlert what="metrics" error={error} />}

      {!isLoading && !error && metrics.length === 0 && (
        <div className="text-center py-6">
          <p className="text-xs text-base-content/40">No metrics found</p>
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
        <div>
          <table className="table table-xs w-full">
            <thead>
              <tr className="text-xs text-base-content/50">
                <th className="w-8">
                  <span className="sr-only">Select</span>
                </th>
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
                    key={`${name}/${metric['sensor:type']}`}
                    className={`cursor-pointer transition-colors ${
                      isSelected
                        ? 'bg-primary/8 border-l-2 border-primary'
                        : 'hover:bg-base-content/5'
                    }`}
                    data-selected={isSelected}
                    onClick={() => handleSelectMetric(metric)}
                  >
                    <td>
                      {/* The metric that is open, as the checkbox of a series is a series that is drawn */}
                      <input
                        type="radio"
                        className="radio radio-primary radio-sm"
                        aria-label={`Select metric ${name}`}
                        checked={isSelected}
                        onChange={() => {}}
                        onClick={(e) => {
                          e.stopPropagation();
                          handleSelectMetric(metric);
                        }}
                      />
                    </td>
                    {/* Takes what the other columns leave, and cuts the name there (`max-w-0` is what lets it) */}
                    <td className="w-2/5 max-w-0">
                      <div className="truncate" title={name}>
                        <span className="font-mono text-xs font-medium">{name}</span>
                        {metric['sensor:unit'] && (
                          <span className="text-xs text-base-content/40 ml-1">({metric['sensor:unit']})</span>
                        )}
                      </div>
                    </td>
                    <td>
                      <span className={`badge badge-sm badge-soft ${sensorTypeBadgeClass(metric['sensor:type'])}`}>
                        {metric['sensor:type'].toLowerCase()}
                      </span>
                    </td>
                    <td className="tabular-nums text-xs">
                      {seriesCount ?? '—'}
                    </td>
                    <td className="hidden sm:table-cell">
                      <div className="flex flex-wrap gap-1 max-w-72">
                        {dimensions.slice(0, 5).map((dim) => (
                          <span key={dim} className="chip font-mono">
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
    </Panel>
  );
}
