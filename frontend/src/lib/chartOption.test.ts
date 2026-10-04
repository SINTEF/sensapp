import { describe, expect, it } from 'vitest';
import { buildChartOption } from './chartOption';
import type { ChartSeries } from './chartOption';

const RANGE = { start: '2026-10-04T00:00:00.000Z', end: '2026-10-04T01:00:00.000Z' };
const temperature: ChartSeries = { name: 'temperature', points: [[1, 20], [2, 21]], boolean: false, color: '#111' };
const door: ChartSeries = { name: 'door', points: [[1, 0], [2, 1]], boolean: true, color: '#222' };

type Option = { series: Array<Record<string, unknown>>; yAxis: Array<Record<string, unknown>>; grid: { right: number } };
const build = (series: ChartSeries[], style: Parameters<typeof buildChartOption>[1] = 'line', log = false) =>
  buildChartOption(series, style, log, RANGE) as unknown as Option;

describe('buildChartOption', () => {
  it('draws a line by default, over the time range', () => {
    const option = build([temperature]);
    expect(option.series[0]).toMatchObject({ type: 'line', name: 'temperature', stack: undefined, areaStyle: undefined, step: undefined });
    expect(buildChartOption([temperature], 'line', false, RANGE)).toMatchObject({
      xAxis: { min: Date.parse(RANGE.start), max: Date.parse(RANGE.end) },
    });
  });

  it('knows its styles', () => {
    expect(build([temperature], 'step').series[0]).toMatchObject({ type: 'line', step: 'end' });
    expect(build([temperature], 'area').series[0]).toMatchObject({ type: 'line', areaStyle: { opacity: 0.2 } });
    expect(build([temperature], 'stacked').series[0]).toMatchObject({ type: 'line', stack: 'total', areaStyle: {} });
    expect(build([temperature], 'bars').series[0]).toMatchObject({ type: 'bar', showSymbol: false });
  });

  it('switches the axis to a logarithmic scale', () => {
    expect(build([temperature]).yAxis[0]).toMatchObject({ type: 'value', scale: true });
    expect(build([temperature], 'line', true).yAxis[0]).toMatchObject({ type: 'log', scale: false });
  });

  it('draws booleans as a step line on an axis of their own, in every style', () => {
    for (const style of ['line', 'area', 'stacked', 'bars'] as const) {
      const option = build([temperature, door], style);
      expect(option.series[1]).toMatchObject({ type: 'line', step: 'end', yAxisIndex: 1 });
      expect(option.series[1]).not.toHaveProperty('stack');
      expect(option.series[1]).not.toHaveProperty('areaStyle');
      expect(option.yAxis).toHaveLength(2);
      expect(option.yAxis[1]).toMatchObject({ min: 0, max: 1 });
      expect(option.grid.right).toBe(60);
    }
  });

  it('has no axis for booleans without booleans', () => {
    const option = build([temperature]);
    expect(option.yAxis).toHaveLength(1);
    expect(option.grid.right).toBe(40);
  });

  it('labels the boolean axis', () => {
    const formatter = (build([door]).yAxis[1].axisLabel as { formatter: (v: number) => string }).formatter;
    expect([formatter(0), formatter(1)]).toEqual(['false', 'true']);
  });
});
