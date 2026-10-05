import { AGGREGATIONS, FIXED_STEPS } from './chartStep';
import type { Aggregation } from './chartStep';
import { CHART_STYLES } from './chartOption';
import type { ChartStyle } from './chartOption';
import { DEFAULT_RANGE, PRESETS } from './timeRange';

/** What the address says, and what the explorer shows from it. */
export interface UrlState {
  metric: string | null;
  series: string[];
  /** A preset such as `24h`, or `null` when the range is `timeRange` */
  relativeRange: string | null;
  timeRange: { start: string; end: string };
  step: string;
  aggregation: Aggregation;
  chartStyle: ChartStyle;
  logScale: boolean;
}

/** The address of a state. What is by default is left out: the plain address is the plain explorer. */
export function toSearchParams(state: UrlState): URLSearchParams {
  const params = new URLSearchParams();
  if (state.metric) params.set('metric', state.metric);
  for (const uuid of state.series) params.append('series', uuid);
  if (state.relativeRange === null) {
    params.set('from', state.timeRange.start);
    params.set('to', state.timeRange.end);
  } else if (state.relativeRange !== DEFAULT_RANGE) {
    params.set('range', state.relativeRange);
  }
  if (state.step !== 'auto') params.set('step', state.step);
  if (state.aggregation !== 'avg') params.set('agg', state.aggregation);
  if (state.chartStyle !== 'line') params.set('style', state.chartStyle);
  if (state.logScale) params.set('log', '1');
  return params;
}

function isDate(value: string | null): value is string {
  return value !== null && !Number.isNaN(Date.parse(value));
}

/** The state of an address. What is wrong or unknown is ignored: an address may be old or typed. */
export function fromSearchParams(params: URLSearchParams): Partial<UrlState> {
  const state: Partial<UrlState> = {};

  const metric = params.get('metric');
  if (metric) state.metric = metric;
  state.series = [...new Set(params.getAll('series').filter(Boolean))];

  const from = params.get('from');
  const to = params.get('to');
  const range = params.get('range');
  if (isDate(from) && isDate(to) && Date.parse(from) < Date.parse(to)) {
    state.relativeRange = null;
    state.timeRange = { start: new Date(from).toISOString(), end: new Date(to).toISOString() };
  } else if (range && PRESETS.some((p) => p.label === range)) {
    state.relativeRange = range;
  }

  const step = params.get('step');
  if (step === 'raw' || (step && FIXED_STEPS.includes(step))) state.step = step;
  const aggregation = params.get('agg');
  if (aggregation && (AGGREGATIONS as readonly string[]).includes(aggregation)) {
    state.aggregation = aggregation as Aggregation;
  }
  const style = params.get('style');
  if (style && (CHART_STYLES as string[]).includes(style)) state.chartStyle = style as ChartStyle;
  if (params.get('log') === '1') state.logScale = true;

  return state;
}
