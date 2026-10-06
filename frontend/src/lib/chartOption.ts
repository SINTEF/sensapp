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

/** 23.99055599 is 24 for the eye. */
function formatValue(value: unknown): string {
  return typeof value === 'number' ? String(Number(value.toPrecision(6))) : String(value ?? '-');
}

function escapeHtml(text: string): string {
  return text.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]!);
}

/** The metric name that every series has (`cpu` of `cpu{host="a"}`), or `''` when they are not all of one metric or have no labels. */
function sharedMetric(names: string[]): string {
  const metric = names[0]?.slice(0, Math.max(names[0].indexOf('{'), 0)) ?? '';
  const same = metric !== '' && names.every((name) => name.startsWith(`${metric}{`));
  return same ? metric : '';
}

/** More rows than this and the tooltip would cover the chart it is about: the largest values stay. */
export const MAX_TOOLTIP_ROWS = 10;

/** What echarts gives the formatter of an axis tooltip, for each series: only what it is used for here. */
export interface TooltipParam {
  /** The dot of the color of the series, as HTML made by echarts */
  marker?: string;
  seriesName?: string;
  value?: unknown;
  axisValueLabel?: string;
}

/**
 * The HTML of the tooltip over a time: the date, and a row for each series. A name that is long (the
 * labels of a series) is cut by the width of the tooltip, never the value after it. Past `MAX_TOOLTIP_ROWS`
 * series, the ones with the largest values are shown and the number of the others is said. When every series
 * is the same metric, its name is said once, in the header, and the rows keep only the labels.
 */
export function tooltipHtml(params: TooltipParam[]): string {
  const metric = sharedMetric(params.map((p) => p.seriesName ?? ''));
  const rows = params.map((p) => {
    const value = Array.isArray(p.value) ? p.value[1] : p.value;
    return { marker: p.marker ?? '', name: (p.seriesName ?? '').slice(metric.length), value };
  });
  const shown =
    rows.length > MAX_TOOLTIP_ROWS
      ? [...rows].sort((a, b) => Number(b.value ?? -Infinity) - Number(a.value ?? -Infinity)).slice(0, MAX_TOOLTIP_ROWS)
      : rows;
  const lines = shown.map(
    (r) =>
      `<div style="display:flex;gap:12px;align-items:center">` +
      `<span style="flex:1 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${r.marker}${escapeHtml(r.name)}</span>` +
      `<b style="flex:none">${escapeHtml(formatValue(r.value))}</b></div>`,
  );
  const more = rows.length - shown.length;
  if (more > 0) lines.push(`<div style="opacity:0.6">and ${more} more</div>`);
  return `<div style="max-width:min(480px,80vw)"><div style="margin-bottom:4px">${escapeHtml([params[0]?.axisValueLabel, metric].filter(Boolean).join(' · '))}</div>${lines.join('')}</div>`;
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
        emphasis: { focus: 'series' as const },
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
      // The series under the pointer in the list stands out, the others fade (EChart sends it)
      emphasis: { focus: 'series' as const },
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
      // Long names must not leave the chart
      confine: true,
      formatter: (params: TooltipParam | TooltipParam[]) => tooltipHtml(Array.isArray(params) ? params : [params]),
    },
    grid: { top: 40, right: hasBoolean ? 60 : 40, bottom: 30, left: 60 },
    xAxis: {
      type: 'time',
      min: new Date(range.start).getTime(),
      max: new Date(range.end).getTime(),
      // On a narrow screen the labels of a short window would sit on one another
      axisLabel: { hideOverlap: true },
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
