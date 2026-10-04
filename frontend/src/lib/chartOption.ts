import type { EChartsCoreOption } from 'echarts/core';

export type ChartStyle = 'line' | 'step' | 'area' | 'stacked' | 'bars';
export const CHART_STYLES: ChartStyle[] = ['line', 'step', 'area', 'stacked', 'bars'];

export interface ChartSeries {
  name: string;
  points: Array<[number, number]>;
  /** Drawn as a 0/1 step line on an axis of its own, whatever the style */
  boolean: boolean;
  color: string;
}

/** The echarts option of the time series chart. */
export function buildChartOption(
  series: ChartSeries[],
  style: ChartStyle,
  logScale: boolean,
  range: { start: string; end: string },
): EChartsCoreOption {
  const hasBoolean = series.some((s) => s.boolean);

  const lines = series.map((s) => {
    if (s.boolean) {
      return {
        name: s.name,
        type: 'line' as const,
        step: 'end' as const,
        yAxisIndex: 1,
        data: s.points,
        showSymbol: false,
        lineStyle: { width: 1.5 },
        color: s.color,
      };
    }
    return {
      name: s.name,
      type: style === 'bars' ? ('bar' as const) : ('line' as const),
      data: s.points,
      smooth: false,
      step: style === 'step' ? ('end' as const) : undefined,
      stack: style === 'stacked' ? 'total' : undefined,
      areaStyle: style === 'area' || style === 'stacked' ? { opacity: 0.2 } : undefined,
      showSymbol: style !== 'bars' && s.points.length < 100,
      symbol: 'circle',
      symbolSize: 3,
      lineStyle: { width: 1.5 },
      color: s.color,
    };
  });

  return {
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { data: series.map((s) => s.name), type: 'scroll', bottom: 0 },
    grid: { top: 40, right: hasBoolean ? 60 : 40, bottom: 60, left: 60 },
    xAxis: {
      type: 'time',
      min: new Date(range.start).getTime(),
      max: new Date(range.end).getTime(),
    },
    yAxis: [
      // A log axis cannot start from the data
      { type: logScale ? 'log' : 'value', scale: !logScale },
      ...(hasBoolean
        ? [
            {
              type: 'value',
              min: 0,
              max: 1,
              interval: 1,
              splitLine: { show: false },
              axisLabel: { formatter: (value: number) => (value ? 'true' : 'false') },
            },
          ]
        : []),
    ],
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', start: 0, end: 100, bottom: 30, height: 20 },
    ],
    toolbox: { feature: { saveAsImage: {}, dataZoom: {}, restore: {} } },
    series: lines,
  };
}
