import { describe, expect, it } from 'vitest';
import { chartStep, isNumericType, resolveStep } from './chartStep';

const START = '2026-10-04T00:00:00.000Z';
const at = (seconds: number) => new Date(Date.parse(START) + seconds * 1000).toISOString();

describe('chartStep', () => {
  it('reads short ranges as they are', () => {
    expect(chartStep(START, at(15 * 60))).toBeUndefined();
    expect(chartStep(START, at(2000))).toBeUndefined();
    expect(chartStep(START, at(3600))).toBeUndefined();
    expect(chartStep(START, at(10_000))).toBeUndefined();
  });

  it('aggregates the ranges of the presets of the UI', () => {
    expect(chartStep(START, at(10_001))).toBe('10s');
    expect(chartStep(START, at(6 * 3600))).toBe('15s');
    expect(chartStep(START, at(24 * 3600))).toBe('1m');
    expect(chartStep(START, at(7 * 86400))).toBe('10m');
    expect(chartStep(START, at(30 * 86400))).toBe('30m');
  });

  it('never makes more than 2000 points, up to a year', () => {
    const seconds = (step: string) => parseInt(step) * { s: 1, m: 60, h: 3600, d: 86400, w: 604800 }[step.slice(-1) as 's']!;
    for (const days of [1, 3, 10, 45, 90, 180, 365]) {
      const step = chartStep(START, at(days * 86400))!;
      expect((days * 86400) / seconds(step)).toBeLessThanOrEqual(2000);
    }
  });

  it('uses the longest step for a range beyond the table', () => {
    expect(chartStep(START, at(20 * 365 * 86400))).toBe('1w');
  });

  it('reads a range that makes no sense as it is', () => {
    expect(chartStep(at(100), START)).toBeUndefined();
    expect(chartStep('not a date', START)).toBeUndefined();
  });
});

describe('isNumericType', () => {
  it('knows what can be averaged', () => {
    for (const type of ['float', 'Float', 'integer', 'numeric']) expect(isNumericType(type)).toBe(true);
    for (const type of ['string', 'boolean', 'location', 'json', 'blob']) expect(isNumericType(type)).toBe(false);
  });
});

describe('resolveStep', () => {
  it('lets the range decide on auto', () => {
    expect(resolveStep('auto', START, at(24 * 3600))).toBe('1m');
    expect(resolveStep('auto', START, at(3600))).toBeUndefined();
  });

  it('does not aggregate raw', () => {
    expect(resolveStep('raw', START, at(30 * 86400))).toBeUndefined();
  });

  it('takes a fixed step as it is, whatever the range', () => {
    expect(resolveStep('5m', START, at(3600))).toBe('5m');
  });
});
