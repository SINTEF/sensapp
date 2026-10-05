import { describe, expect, it } from 'vitest';
import { fromSearchParams, toSearchParams } from './urlState';
import type { UrlState } from './urlState';

const RANGE = { start: '2026-10-04T10:00:00.000Z', end: '2026-10-04T11:00:00.000Z' };
const plain: UrlState = {
  metric: null,
  series: [],
  relativeRange: '1h',
  timeRange: RANGE,
  step: 'auto',
  aggregation: 'avg',
  chartStyle: 'line',
  logScale: false,
};

const parse = (query: string) => fromSearchParams(new URLSearchParams(query));

describe('toSearchParams', () => {
  it('leaves the plain explorer with a plain address', () => {
    expect(toSearchParams(plain).toString()).toBe('');
  });

  it('writes what was chosen', () => {
    const params = toSearchParams({
      ...plain,
      metric: 'temperature value',
      series: ['a-1', 'b-2'],
      relativeRange: '24h',
      step: '5m',
      aggregation: 'max',
      chartStyle: 'stacked',
      logScale: true,
    });
    expect(params.get('metric')).toBe('temperature value');
    expect(params.getAll('series')).toEqual(['a-1', 'b-2']);
    expect(params.get('range')).toBe('24h');
    expect(params.get('step')).toBe('5m');
    expect(params.get('agg')).toBe('max');
    expect(params.get('style')).toBe('stacked');
    expect(params.get('log')).toBe('1');
    expect(params.has('from')).toBe(false);
  });

  it('writes the dates once they were typed', () => {
    const params = toSearchParams({ ...plain, relativeRange: null });
    expect(params.get('from')).toBe(RANGE.start);
    expect(params.get('to')).toBe(RANGE.end);
    expect(params.has('range')).toBe(false);
  });
});

describe('fromSearchParams', () => {
  it('reads back what was written', () => {
    const state: UrlState = {
      metric: 'cpu',
      series: ['a', 'b'],
      relativeRange: null,
      timeRange: RANGE,
      step: 'raw',
      aggregation: 'last',
      chartStyle: 'bars',
      logScale: true,
    };
    expect(fromSearchParams(toSearchParams(state))).toEqual(state);
  });

  it('reads a preset', () => {
    expect(parse('range=7d')).toMatchObject({ relativeRange: '7d' });
  });

  it('ignores what makes no sense', () => {
    const state = parse('range=2y&step=7x&agg=median&style=pie&log=yes&from=nope&to=2026-10-04T11:00:00Z');
    expect(state).toEqual({ series: [] });
  });

  it('ignores dates in the wrong order', () => {
    expect(parse('from=2026-10-04T11:00:00Z&to=2026-10-04T10:00:00Z')).toEqual({ series: [] });
  });

  it('asks for a series once', () => {
    expect(parse('series=a&series=a&series=b&series=').series).toEqual(['a', 'b']);
  });
});
