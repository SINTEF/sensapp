import { describe, expect, it } from 'vitest';
import { buildChartOption, MAX_TOOLTIP_ROWS, tooltipHtml } from './chartOption';
import type { ChartSeries } from './chartOption';

const RANGE = { start: '2026-10-04T00:00:00.000Z', end: '2026-10-04T01:00:00.000Z' };
const temperature: ChartSeries = { id: 'uuid-t', name: 'temperature', points: [[1, 20], [2, 21]], boolean: false, color: '#111' };
const door: ChartSeries = { id: 'uuid-d', name: 'door', points: [[1, 0], [2, 1]], boolean: true, color: '#222' };

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

  it('hides the labels of the time axis that would overlap (a short window on a narrow screen)', () => {
    expect(buildChartOption([temperature], 'line', false, RANGE)).toMatchObject({
      xAxis: { axisLabel: { hideOverlap: true } },
    });
  });

  it('has no legend (the list of series has the colors), and tells its series by id', () => {
    const option = build([temperature, door]) as unknown as { legend?: unknown };
    expect(option.legend).toBeUndefined();
    expect(build([temperature, door]).series.map((s) => s.id)).toEqual(['uuid-t', 'uuid-d']);
  });

  it('has no zoom of its own: the window is the explorer\'s, a drag on the chart asks for another', () => {
    const option = buildChartOption([temperature], 'line', false, RANGE) as unknown as {
      dataZoom?: unknown;
      brush: Record<string, unknown>;
    };
    expect(option.dataZoom).toBeUndefined();
    expect(option.brush).toMatchObject({ xAxisIndex: 0, brushType: 'lineX', brushMode: 'single' });
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

describe('tooltipHtml', () => {
  const row = (name: string, value: unknown, axisValueLabel = '2026-10-04') => ({ marker: '<i></i>', seriesName: name, value, axisValueLabel });

  it('says the date, then each series with its value rounded for the eye', () => {
    const html = tooltipHtml([row('temperature', [1, 23.99055599]), row('door', [1, 1])]);
    expect(html).toContain('2026-10-04');
    expect(html).toMatch(/temperature<\/span><b[^>]*>23\.9906<\/b>/);
    expect(html).toMatch(/door<\/span><b[^>]*>1<\/b>/);
    expect(html).not.toContain('more');
  });

  it('says the metric once in the header when all the series are of it, and keeps the labels in the rows', () => {
    const html = tooltipHtml([row('cpu{host="a"}', [1, 1]), row('cpu{host="b"}', [1, 2])]);
    expect(html).toContain('2026-10-04 · cpu</div>');
    expect(html).toContain('{host=&quot;a&quot;}');
    expect(html).not.toContain('>cpu{');
  });

  it('keeps the whole names when the metrics differ or have no labels', () => {
    const mixed = tooltipHtml([row('cpu{host="a"}', [1, 1]), row('mem{host="a"}', [1, 2])]);
    expect(mixed).toContain('cpu{host=');
    expect(mixed).toContain('mem{host=');
    const bare = tooltipHtml([row('temperature', [1, 1]), row('door', [1, 2])]);
    expect(bare).toContain('temperature');
    expect(bare).not.toContain(' · ');
  });

  it('escapes the names: they are the labels of the series', () => {
    const html = tooltipHtml([row('<i>cpu{host="<b>a</b>"}', [1, 1])]);
    expect(html).toContain('&lt;i&gt;cpu</div>');
    expect(html).toContain('{host=&quot;&lt;b&gt;a&lt;/b&gt;&quot;}');
    expect(html).not.toContain('<b>a</b>');
    expect(html).not.toContain('<i>cpu');
  });

  it('cuts a long name by the width of the tooltip, not the value', () => {
    const html = tooltipHtml([row('x'.repeat(500), [1, 1])]);
    expect(html).toContain('text-overflow:ellipsis');
    expect(html).toContain('max-width:min(480px,80vw)');
  });

  it('keeps the largest values past the row limit, and says how many are left out', () => {
    const many = Array.from({ length: MAX_TOOLTIP_ROWS + 5 }, (_, i) => row(`series-${i}`, [1, i]));
    const html = tooltipHtml(many);
    expect(html.match(/<b style/g)).toHaveLength(MAX_TOOLTIP_ROWS);
    expect(html).toContain('series-14');
    expect(html).not.toContain('series-4<');
    expect(html).toContain('and 5 more');
  });

  it('keeps the order of the series when they all fit', () => {
    const html = tooltipHtml([row('b', [1, 1]), row('a', [1, 9])]);
    expect(html.indexOf('>b<')).toBeLessThan(html.indexOf('>a<'));
  });

  it('shows a dash for a series without a value', () => {
    expect(tooltipHtml([row('gap', [1, undefined])])).toMatch(/<b[^>]*>-<\/b>/);
  });
});
