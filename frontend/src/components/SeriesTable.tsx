import { useState } from 'react';
import { nextBookmark, useSeries } from '../hooks/useSeries';
import type { SeriesDataset } from '../hooks/useSeries';
import { useSelectionStore } from '../stores/useSelectionStore';
import { ErrorAlert, Loading } from './Feedback';

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
  // The cursor of every page shown so far: the server only goes forward, so Previous pops one
  const [bookmarks, setBookmarks] = useState<string[]>([]);

  const selector = selectorInput || undefined;

  const { data, isLoading, error } = useSeries({
    metric: selectedMetric || undefined,
    selector,
    bookmark: bookmarks.at(-1),
  });
  const next = nextBookmark(data);

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
    <div className="flex flex-col h-full gap-2">
      <div className="flex flex-wrap gap-2 items-center shrink-0">
        <input
          type="text"
          placeholder='{env="prod", region=~"us.*"}'
          className="input input-bordered input-xs font-mono text-xs flex-1 min-w-44 max-w-xs h-7"
          value={selectorInput}
          onChange={(e) => {
            setSelectorInput(e.target.value);
            setBookmarks([]);
          }}
        />
        <input
          type="text"
          placeholder="Quick filter..."
          className="input input-bordered input-xs text-xs min-w-28 max-w-40 h-7"
          value={labelFilter}
          onChange={(e) => setLabelFilter(e.target.value)}
        />
        {selectedSeries.length > 0 && (
          <span className="text-xs text-primary font-medium">
            {selectedSeries.length} selected
          </span>
        )}
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto overflow-x-auto">
      {isLoading && <Loading />}

      {error && <ErrorAlert what="series" error={error} />}

      {!isLoading && !error && filteredSeries.length === 0 && (
        <div className="text-center py-6">
          <p className="text-xs text-base-content/40">
            {series.length === 0 ? 'No series found for this metric' : 'No series match the current filter'}
          </p>
        </div>
      )}

      {!isLoading && !error && filteredSeries.length > 0 && (
        <div>
          <table className="table table-xs w-full">
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
                        aria-label={`Select series ${s['dct:identifier']}`}
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
          <div className="sticky bottom-0 bg-base-100 flex items-center justify-between pt-2 text-xs text-base-content/40">
            <span>
              {filteredSeries.length} series
              {filteredSeries.length !== series.length && ` (${series.length} on this page)`}
            </span>
            {(bookmarks.length > 0 || next) && (
              <div className="join">
                <button
                  className="join-item btn btn-xs btn-outline"
                  aria-label="Previous page"
                  disabled={bookmarks.length === 0}
                  onClick={() => setBookmarks(bookmarks.slice(0, -1))}
                >
                  ‹
                </button>
                <button
                  className="join-item btn btn-xs btn-outline"
                  aria-label="Next page"
                  disabled={!next}
                  onClick={() => next && setBookmarks([...bookmarks, next])}
                >
                  ›
                </button>
              </div>
            )}
          </div>
        </div>
      )}
      </div>
    </div>
  );
}
