import { lazy, Suspense, useMemo } from 'react';
import { keepPreviousData, useQueries } from '@tanstack/react-query';
import { getSeriesData } from '../client';
import { useSelectionStore } from '../stores/useSelectionStore';
import { unwrap } from '../api/clientConfig';
import { isBooleanType, isNumericType, resolveStep } from '../lib/chartStep';
import { buildChartOption } from '../lib/chartOption';

// echarts is large: load it when the first chart is drawn.
const EChart = lazy(() => import('./EChart'));

// Color palette for multiple series
const COLORS = [
  '#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6',
  '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#6366f1',
];

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

export function TimeSeriesChart() {
  const { selectedSeries, timeRange, step: stepChoice, aggregation, chartStyle, logScale } =
    useSelectionStore();

  const step = resolveStep(stepChoice, timeRange.start, timeRange.end);

  const queries = useQueries({
    queries: selectedSeries.map((s, index) => ({
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
        return {
          records: unwrap(result) as unknown as SenMLRecord[],
          series: s,
          colorIndex: index,
        };
      },
      enabled: !!s.uuid,
      // The range moves every minute: the chart keeps what it shows until the new data is there
      placeholderData: keepPreviousData,
    })),
  });

  const isLoading = queries.some((q) => q.isLoading);
  const errors = queries.filter((q) => q.error);

  const option = useMemo(() => {
    const series = queries
      // What is kept while loading belongs to the series that had this place: not to another
      .filter((q, i) => q.data && q.data.series.uuid === selectedSeries[i]?.uuid)
      .map((q) => {
        const { records, series, colorIndex } = q.data!;
        return {
          name: buildSeriesLabel(series.name, series.labels),
          points: parseSenMLToTimeSeries(records),
          boolean: isBooleanType(series.type),
          color: COLORS[colorIndex % COLORS.length],
        };
      });
    return buildChartOption(series, chartStyle, logScale, timeRange);
  }, [queries, selectedSeries, timeRange, chartStyle, logScale]);

  if (selectedSeries.length === 0) {
    return null;
  }

  return (
    <div className="flex flex-col h-full">
      {isLoading && (
        <div className="flex items-center justify-center gap-2 py-2 shrink-0">
          <span className="loading loading-spinner loading-sm text-primary" />
          <span className="text-sm text-base-content/50">Loading chart data...</span>
        </div>
      )}

      {errors.length > 0 && (
        <div className="alert alert-warning shrink-0">
          <span>Some series failed to load: {errors.map(q => q.error instanceof Error ? q.error.message : 'Unknown error').join(', ')}</span>
        </div>
      )}

      <div className="flex-1 min-h-0">
        <Suspense fallback={null}>
          <EChart option={option} />
        </Suspense>
      </div>
    </div>
  );
}
