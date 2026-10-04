/** Longest raw read of `GET /series/{uuid}` is 100 000 samples: wide ranges must be aggregated. */
const MAX_POINTS = 2000;

/** Below this step the samples are read as they are: averaging 5 seconds hides nothing. */
const MIN_STEP_SECONDS = 5;

/** Round steps, in seconds, with the Prometheus duration that says them. */
const STEPS: Array<[number, string]> = [
  [5, '5s'],
  [10, '10s'],
  [15, '15s'],
  [30, '30s'],
  [60, '1m'],
  [120, '2m'],
  [300, '5m'],
  [600, '10m'],
  [900, '15m'],
  [1800, '30m'],
  [3600, '1h'],
  [7200, '2h'],
  [10800, '3h'],
  [21600, '6h'],
  [43200, '12h'],
  [86400, '1d'],
  [604800, '1w'],
];

const NUMERIC_TYPES = ['float', 'integer', 'numeric'];

/** Only numbers can be averaged: the server refuses (or fails) on the other types. */
export function isNumericType(type: string): boolean {
  return NUMERIC_TYPES.includes(type.toLowerCase());
}

/**
 * The `step` that keeps a chart of this range under `MAX_POINTS` points, or `undefined` when the
 * range is short enough (under about 2.8 hours) to be read as it is. The samples of a step are
 * averaged.
 */
export function chartStep(start: string, end: string): string | undefined {
  const seconds = (new Date(end).getTime() - new Date(start).getTime()) / 1000;
  if (!(seconds > MAX_POINTS)) return undefined;
  const wanted = seconds / MAX_POINTS;
  if (wanted <= MIN_STEP_SECONDS) return undefined;
  const [, step] = STEPS.find(([length]) => length >= wanted) ?? STEPS[STEPS.length - 1];
  return step;
}
