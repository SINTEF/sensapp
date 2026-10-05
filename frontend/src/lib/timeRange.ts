/** The ranges of the preset buttons, which are relative to now. */
export const PRESETS = [
  { label: '15m', minutes: 15 },
  { label: '1h', minutes: 60 },
  { label: '6h', minutes: 360 },
  { label: '24h', minutes: 1440 },
  { label: '7d', minutes: 10080 },
  { label: '30d', minutes: 43200 },
];

export const DEFAULT_RANGE = '1h';

/** The range of a preset ending now, or `undefined` for a label that is none. */
export function rangeFor(label: string, now = new Date()): { start: string; end: string } | undefined {
  const preset = PRESETS.find((p) => p.label === label);
  if (!preset) return undefined;
  return {
    start: new Date(now.getTime() - preset.minutes * 60_000).toISOString(),
    end: now.toISOString(),
  };
}

export interface TimeRange {
  start: string;
  end: string;
}

/** The smallest window a brush can ask for: a second, the finest step there is. */
export const MIN_WINDOW_MS = 1000;

/** The window of a brush on the chart, in whole seconds. `undefined` when it is too narrow to mean something. */
export function brushedRange(fromMs: number, toMs: number): TimeRange | undefined {
  // A drag of less than a second is a slip of the hand, not a window
  if (!(Math.abs(toMs - fromMs) >= MIN_WINDOW_MS)) return undefined;
  const start = Math.floor(Math.min(fromMs, toMs) / 1000) * 1000;
  const end = Math.ceil(Math.max(fromMs, toMs) / 1000) * 1000;
  return { start: new Date(start).toISOString(), end: new Date(end).toISOString() };
}

/** The window moved by half its width, earlier (`-1`) or later (`1`). It never goes past now. */
export function panned(range: TimeRange, direction: -1 | 1, now = new Date()): TimeRange {
  const start = Date.parse(range.start);
  const end = Date.parse(range.end);
  let shift = direction * ((end - start) / 2);
  if (end + shift > now.getTime()) shift = Math.max(0, now.getTime() - end);
  return { start: new Date(start + shift).toISOString(), end: new Date(end + shift).toISOString() };
}

/** Twice the window around its middle. The end stays at now at the latest, the window gets longer on the other side. */
export function zoomedOut(range: TimeRange, now = new Date()): TimeRange {
  const start = Date.parse(range.start);
  const end = Date.parse(range.end);
  const width = end - start;
  const newEnd = Math.min(end + width / 2, now.getTime());
  return { start: new Date(newEnd - 2 * width).toISOString(), end: new Date(newEnd).toISOString() };
}

/** The preset after this one, or the same one for the widest. */
export function nextPreset(label: string): string {
  const index = PRESETS.findIndex((p) => p.label === label);
  return PRESETS[Math.min(index + 1, PRESETS.length - 1)].label;
}
