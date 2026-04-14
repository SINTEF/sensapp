import { useMemo } from 'react';
import ReactECharts from 'echarts-for-react';
import { useQueries } from '@tanstack/react-query';
import { getSeriesData } from '../client';
import { useSelectionStore } from '../stores/useSelectionStore';
import { extractErrorMessage } from '../api/clientConfig';
import type { SenMLRecord } from '../hooks/useSeriesData';

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

function parseSenMLToTimeSeries(
  records: SenMLRecord[]
): Array<[number, number]> {
  let baseName = '';
  let baseTime = 0;

  const points: Array<[number, number]> = [];

  for (const rec of records) {
    if (rec.bn !== undefined) baseName = rec.bn;
    if (rec.bt !== undefined) baseTime = rec.bt;

    // We only care about re-assigning baseName for multi-record packs
    void baseName;

    const time = (baseTime + (rec.t ?? 0)) * 1000; // Convert to ms
    const value = rec.v ?? (rec.vs ? parseFloat(rec.vs) : NaN);

    if (!isNaN(value) && isFinite(time)) {
      points.push([time, value]);
    }
  }

  return points.sort((a, b) => a[0] - b[0]);
}

export function TimeSeriesChart() {
  const { selectedSeries, timeRange } = useSelectionStore();

  const queries = useQueries({
    queries: selectedSeries.map((s, index) => ({
      queryKey: ['seriesData', s.uuid, timeRange] as const,
      queryFn: async () => {
        const result = await getSeriesData({
          path: { series_uuid: s.uuid },
          query: {
            format: 'senml',
            start: timeRange.start,
            end: timeRange.end,
          },
        });
        if (result.error) {
          throw new Error(extractErrorMessage(result.error));
        }
        return {
          records: result.data as unknown as SenMLRecord[],
          series: s,
          colorIndex: index,
        };
      },
      enabled: !!s.uuid,
    })),
  });

  const isLoading = queries.some((q) => q.isLoading);
  const errors = queries.filter((q) => q.error);

  const option = useMemo(() => {
    const seriesData = queries
      .filter((q) => q.data)
      .map((q) => {
        const { records, series, colorIndex } = q.data!;
        const points = parseSenMLToTimeSeries(records);
        return {
          name: buildSeriesLabel(series.name, series.labels),
          type: 'line' as const,
          data: points,
          smooth: false,
          showSymbol: points.length < 100,
          symbol: 'circle',
          symbolSize: 3,
          lineStyle: { width: 1.5 },
          color: COLORS[colorIndex % COLORS.length],
        };
      });

    return {
      tooltip: {
        trigger: 'axis' as const,
        axisPointer: {
          type: 'cross' as const,
        },
      },
      legend: {
        data: seriesData.map((s) => s.name),
        type: 'scroll' as const,
        bottom: 0,
      },
      grid: {
        top: 40,
        right: 40,
        bottom: 60,
        left: 60,
      },
      xAxis: {
        type: 'time' as const,
        min: new Date(timeRange.start).getTime(),
        max: new Date(timeRange.end).getTime(),
      },
      yAxis: {
        type: 'value' as const,
        scale: true,
      },
      dataZoom: [
        {
          type: 'inside' as const,
          start: 0,
          end: 100,
        },
        {
          type: 'slider' as const,
          start: 0,
          end: 100,
          bottom: 30,
          height: 20,
        },
      ],
      toolbox: {
        feature: {
          saveAsImage: {},
          dataZoom: {},
          restore: {},
        },
      },
      series: seriesData,
    };
  }, [queries, timeRange]);

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

      <ReactECharts
        option={option}
        style={{ height: '100%', width: '100%' }}
        notMerge={true}
        lazyUpdate={true}
      />
    </div>
  );
}
