import { useEffect, useRef, useState } from 'react';
import { nextBookmark, useSeries } from '../hooks/useSeries';
import type { SeriesDataset } from '../hooks/useSeries';
import { isBooleanType, isNumericType } from '../lib/chartStep';
import { seriesColor, SLOTS } from '../lib/palette';
import { usePrefersDark } from '../lib/usePrefersDark';
import { useSelectionStore } from '../stores/useSelectionStore';
import type { SeriesInfo } from '../stores/useSelectionStore';
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

/** A metric of at most this many series is shown whole when it is selected: there is a color for each. */
const AUTO_SELECT_MAX = SLOTS;

function toSeriesInfo(dataset: SeriesDataset): SeriesInfo {
  return {
    uuid: dataset['dct:identifier'],
    name: dataset['dct:title'],
    labels: labelsToRecord(dataset['sensor:labels']),
    type: dataset['sensor:type'],
  };
}

export function SeriesTable() {
  const {
    selectedMetric,
    selectedSeries,
    toggleSeries,
    selectSeries,
    labelFilter,
    setLabelFilter,
  } = useSelectionStore();
  const dark = usePrefersDark();
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

  // A metric that is not too big is shown whole, once: clearing it afterwards is the choice of the
  // user. What the address already shows is left alone, and so are the series that cannot be drawn.
  const autoSelected = useRef(false);
  useEffect(() => {
    if (!data || autoSelected.current) return;
    autoSelected.current = true;
    const all = data['dcat:dataset'];
    if (nextBookmark(data) || all.length > AUTO_SELECT_MAX) return;
    if (useSelectionStore.getState().selectedSeries.length > 0) return;
    selectSeries(
      all
        .map(toSeriesInfo)
        .filter((s) => isNumericType(s.type) || isBooleanType(s.type)),
    );
  }, [data, selectSeries]);

  function selectedOf(dataset: SeriesDataset) {
    return selectedSeries.find((s) => s.uuid === dataset['dct:identifier']);
  }

  // One column per label dimension
  const dimensions = [...new Set(series.flatMap((s) => Object.keys(labelsToRecord(s['sensor:labels']))))].sort();
  // The type is said when there is something to tell: a name can exist with more than one
  const mixedTypes = new Set(series.map((s) => s['sensor:type'])).size > 1;

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
                <th className="w-12 font-medium">
                  <span className="sr-only">Select</span>
                </th>
                {dimensions.map((dimension) => (
                  <th key={dimension} className="font-medium">
                    {dimension}
                  </th>
                ))}
                {dimensions.length === 0 && <th className="font-medium">Series</th>}
                {mixedTypes && <th className="font-medium">Type</th>}
                <th className="font-medium text-right">
                  <span className="sr-only">Series ID</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {filteredSeries.map((s) => {
                const uuid = s['dct:identifier'];
                const selected = selectedOf(s);
                const labels = labelsToRecord(s['sensor:labels']);
                return (
                  <tr
                    key={uuid}
                    className={`cursor-pointer transition-colors ${
                      selected
                        ? 'bg-primary/8'
                        : 'hover:bg-base-200/60'
                    }`}
                    onClick={() => toggleSeries(toSeriesInfo(s))}
                  >
                    <td>
                      <div className="flex items-center gap-1.5">
                        <input
                          type="checkbox"
                          className="checkbox checkbox-primary checkbox-xs"
                          aria-label={`Select series ${uuid}`}
                          checked={!!selected}
                          onChange={() => toggleSeries(toSeriesInfo(s))}
                          onClick={(e) => e.stopPropagation()}
                        />
                        <span
                          className="inline-block w-2.5 h-2.5 rounded-full shrink-0"
                          data-testid={selected ? 'series-color' : undefined}
                          style={{ background: selected ? seriesColor(selected.slot, dark) : 'transparent' }}
                        />
                      </div>
                    </td>
                    {dimensions.map((dimension) => (
                      <td key={dimension} className="text-xs max-w-40 truncate" title={labels[dimension]}>
                        {labels[dimension] ?? <span className="text-base-content/30">—</span>}
                      </td>
                    ))}
                    {dimensions.length === 0 && (
                      <td className="text-xs text-base-content/30">no labels</td>
                    )}
                    {mixedTypes && (
                      <td>
                        <span className="badge badge-xs badge-outline">{s['sensor:type']}</span>
                      </td>
                    )}
                    <td
                      className="font-mono text-[10px] text-base-content/40 text-right max-w-28 truncate"
                      title={uuid}
                    >
                      {uuid}
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
