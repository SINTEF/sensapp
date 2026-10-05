import { describe, expect, it } from 'vitest';
import { freeSlot, readableOn, SERIES_COLORS, seriesColor, SLOTS } from './palette';

describe('palette', () => {
  it('has a step of each hue for each theme', () => {
    expect(SERIES_COLORS.light).toHaveLength(SLOTS);
    expect(SERIES_COLORS.dark).toHaveLength(SLOTS);
    expect(seriesColor(0, false)).not.toBe(seriesColor(0, true));
  });

  it('gives the first slot nobody uses', () => {
    expect(freeSlot([])).toBe(0);
    expect(freeSlot([0, 1])).toBe(2);
    expect(freeSlot([0, 2])).toBe(1);
  });

  it('uses the colors again when they are all taken', () => {
    const all = Array.from({ length: SLOTS }, (_, i) => i);
    expect(freeSlot(all)).toBe(0);
    expect(seriesColor(SLOTS + 1, false)).toBe(seriesColor(1, false));
  });

  it('writes in dark on the light colors and in white on the others', () => {
    expect(readableOn('#eda100')).toBe('#111827');
    expect(readableOn('#2a78d6')).toBe('#ffffff');
  });
});
