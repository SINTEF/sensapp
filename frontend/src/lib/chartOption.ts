import type { EChartsCoreOption } from 'echarts/core';

export type ChartStyle = 'line' | 'step' | 'area' | 'stacked' | 'bars';
export const CHART_STYLES: ChartStyle[] = ['line', 'step', 'area', 'stacked', 'bars'];

export interface ChartSeries {
  /** What echarts tells the series by when the option changes: a series that stays is not redrawn */
  id: string;
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
        id: s.id,
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
      id: s.id,
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
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      // Long names must not leave the chart, and 23.99055599 is 24 for the eye
      confine: true,
      valueFormatter: (value: unknown) => (typeof value === 'number' ? String(Number(value.toPrecision(6))) : String(value ?? '-')),
    },
    grid: { top: 40, right: hasBoolean ? 60 : 40, bottom: 30, left: 60 },
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
    // The time window is the one of the explorer, and a drag on the chart asks for another (EChart
    // listens to it): there is no zoom of the chart on its own, which would zoom into points that
    // were cut for the window before.
    brush: {
      xAxisIndex: 0,
      brushType: 'lineX',
      brushMode: 'single',
      transformable: false,
      removeOnClick: true,
      brushStyle: { borderWidth: 1, color: 'rgba(100, 150, 220, 0.15)', borderColor: 'rgba(100, 150, 220, 0.7)' },
    },
    // echarts brings buttons for the brush with it (a drag does it, and the header says so)
    toolbox: { show: false },
    series: lines,
  };
}
