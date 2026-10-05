import { lazy, Suspense, useMemo, useState } from 'react';
import { useQueries } from '@tanstack/react-query';
import { getSeriesData } from '../client';
import { useSelectionStore } from '../stores/useSelectionStore';
import { unwrap } from '../api/clientConfig';
import { isBooleanType, isNumericType, resolveStep } from '../lib/chartStep';
import { buildChartOption } from '../lib/chartOption';
import { seriesColor } from '../lib/palette';
import { sharedLabels, withoutShared } from '../lib/seriesLabels';
import { brushedRange } from '../lib/timeRange';
import { usePrefersDark } from '../lib/usePrefersDark';

// echarts is large: load it when the first chart is drawn.
const EChart = lazy(() => import('./EChart'));

function buildSeriesLabel(name: string, labels: Record<string, string>): string {
  const labelStr = Object.entries(labels)
    .map(([k, v]) => `${k}="${v}"`)
    .join(', ');
  return labelStr ? `${name}{${labelStr}}` : name;
}

/** One SenML record. Only the first of a pack has the base time (`bt`), the others are relative to it. */
interface SenMLRecord {
  bt?: number;
  t?: number;
  v?: number;
  vs?: string;
  vb?: boolean;
}

function parseSenMLToTimeSeries(
  records: SenMLRecord[]
): Array<[number, number]> {
  let baseTime = 0;

  const points: Array<[number, number]> = [];

  for (const rec of records) {
    if (rec.bt !== undefined) baseTime = rec.bt;

    const time = (baseTime + (rec.t ?? 0)) * 1000; // Convert to ms
    // A boolean is 0 or 1
    const value = rec.vb !== undefined ? Number(rec.vb) : (rec.v ?? (rec.vs ? parseFloat(rec.vs) : NaN));

    if (!isNaN(value) && isFinite(time)) {
      points.push([time, value]);
    }
  }

  return points.sort((a, b) => a[0] - b[0]);
}

/** What a query returns: the records of a series. */
interface Loaded {
  uuid: string;
  records: SenMLRecord[];
}

export function TimeSeriesChart() {
  const { selectedSeries, timeRange, setTimeRange, step: stepChoice, aggregation, chartStyle, logScale } =
    useSelectionStore();
  const hoveredSeries = useSelectionStore((state) => state.hoveredSeries);
  const dark = usePrefersDark();

  const step = resolveStep(stepChoice, timeRange.start, timeRange.end);

  const queries = useQueries({
    queries: selectedSeries.map((s) => ({
      queryKey: ['seriesData', s.uuid, timeRange, step, aggregation] as const,
      queryFn: async () => {
        const result = await getSeriesData({
          path: { series_uuid: s.uuid },
          query: {
            format: 'senml',
            start: timeRange.start,
            end: timeRange.end,
            // Only numbers can be aggregated. The server refuses more than 100 000 raw samples.
            ...(step && isNumericType(s.type) ? { step, aggregation } : {}),
          },
        });
        return { uuid: s.uuid, records: unwrap(result) as unknown as SenMLRecord[] };
      },
      enabled: !!s.uuid,
    })),
  });

  const isFetching = queries.some((q) => q.isFetching);
  const errors = queries.filter((q) => q.error);

  // The range moves every minute and a query has a key per range: the chart keeps what it shows
  // until the data of the new range is there. (`placeholderData` cannot do it for `useQueries`.)
  const [shown, setShown] = useState<Record<string, Loaded>>({});
  const loaded = queries.flatMap((q) => (q.data ? [q.data] : []));
  if (loaded.some((data) => shown[data.uuid] !== data)) {
    setShown({ ...shown, ...Object.fromEntries(loaded.map((data) => [data.uuid, data])) });
  }

  const option = useMemo(() => {
    // What every series has is not what tells them apart
    const shared = sharedLabels(selectedSeries.map((s) => s.labels));
    const series = selectedSeries
      .filter((s) => shown[s.uuid])
      .map((s) => {
        return {
          id: s.uuid,
          name: buildSeriesLabel(s.name, withoutShared(s.labels, shared)),
          points: parseSenMLToTimeSeries(shown[s.uuid].records),
          boolean: isBooleanType(s.type),
          color: seriesColor(s.slot, dark),
        };
      });
    return buildChartOption(series, chartStyle, logScale, timeRange);
  }, [shown, selectedSeries, dark, timeRange, chartStyle, logScale]);

  // A drag on the chart is the window to show
  function handleBrush(fromMs: number, toMs: number) {
    const range = brushedRange(fromMs, toMs);
    if (range) setTimeRange(range.start, range.end);
  }

  if (selectedSeries.length === 0) {
    return null;
  }

  return (
    <div className="flex flex-col h-full">
      {errors.length > 0 && (
        <div className="alert alert-warning shrink-0">
          <span>Some series failed to load: {errors.map(q => q.error instanceof Error ? q.error.message : 'Unknown error').join(', ')}</span>
        </div>
      )}

      <div className="flex-1 min-h-0 relative">
        {/* Over the chart, so that the chart does not move when it loads */}
        {isFetching && (
          <progress
            className="progress progress-primary absolute top-0 left-0 w-full h-0.5 z-10"
            aria-label="Loading chart data"
          />
        )}
        <Suspense fallback={null}>
          <EChart option={option} onBrush={handleBrush} highlight={hoveredSeries} />
        </Suspense>
      </div>
    </div>
  );
}
