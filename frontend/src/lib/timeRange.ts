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
