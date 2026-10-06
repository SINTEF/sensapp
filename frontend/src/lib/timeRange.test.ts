import { describe, expect, it } from 'vitest';
import { brushedRange, nextPreset, panned, rangeFor, zoomedOut } from './timeRange';

const NOW = new Date('2026-10-05T12:00:00.000Z');
const window = (start: string, end: string) => ({ start: `2026-10-05T${start}:00.000Z`, end: `2026-10-05T${end}:00.000Z` });

describe('rangeFor', () => {
  it('ends now', () => {
    expect(rangeFor('1h', NOW)).toEqual(window('11:00', '12:00'));
    expect(rangeFor('nope', NOW)).toBeUndefined();
  });
});

describe('brushedRange', () => {
  it('is in whole seconds, outwards, whichever way the drag went', () => {
    const expected = { start: '2026-10-05T10:00:01.000Z', end: '2026-10-05T10:00:05.000Z' };
    expect(brushedRange(Date.parse('2026-10-05T10:00:01.900Z'), Date.parse('2026-10-05T10:00:04.100Z'))).toEqual(expected);
    expect(brushedRange(Date.parse('2026-10-05T10:00:04.100Z'), Date.parse('2026-10-05T10:00:01.900Z'))).toEqual(expected);
  });

  it('is nothing for a click, or for what is not a time', () => {
    const t = Date.parse('2026-10-05T10:00:01.000Z');
    expect(brushedRange(t, t)).toBeUndefined();
    expect(brushedRange(t, t + 400)).toBeUndefined();
    expect(brushedRange(NaN, t)).toBeUndefined();
  });
});

describe('panned', () => {
  it('moves by half a window', () => {
    expect(panned(window('10:00', '11:00'), -1, NOW)).toEqual(window('09:30', '10:30'));
    expect(panned(window('10:00', '11:00'), 1, NOW)).toEqual(window('10:30', '11:30'));
  });

  it('does not go past now', () => {
    expect(panned(window('10:30', '11:50'), 1, NOW)).toEqual(window('10:40', '12:00'));
    expect(panned(window('11:00', '12:00'), 1, NOW)).toEqual(window('11:00', '12:00'));
  });
});

describe('zoomedOut', () => {
  it('is twice the window around its middle', () => {
    expect(zoomedOut(window('10:00', '11:00'), NOW)).toEqual(window('09:30', '11:30'));
  });

  it('keeps the end at now at the latest, and takes the rest from before', () => {
    expect(zoomedOut(window('11:00', '12:00'), NOW)).toEqual(window('10:00', '12:00'));
  });
});

describe('nextPreset', () => {
  it('is the next one, and the widest stays', () => {
    expect(nextPreset('1h')).toBe('6h');
    expect(nextPreset('30d')).toBe('1y');
    expect(nextPreset('1y')).toBe('1y');
  });
});
