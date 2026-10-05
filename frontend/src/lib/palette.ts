/**
 * The colors of the series, in the order they are given: eight hues, one step for each theme (the
 * steps of the light theme are too light on the dark surface). Checked with the dataviz skill's
 * `validate_palette.js`: worst neighbours 9.1 (light) and 8.4 (dark) of color-blind distance, 19+ for
 * normal vision, 3:1 on the dark surface. On the light surface three are under 3:1: the series list
 * says which color is which, so a color is never the only way to tell the series apart.
 */
export const SERIES_COLORS = {
  light: ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948'],
  dark: ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300', '#9085e9', '#e66767'],
};

export const SLOTS = SERIES_COLORS.light.length;

export function seriesColor(slot: number, dark: boolean): string {
  const colors = dark ? SERIES_COLORS.dark : SERIES_COLORS.light;
  return colors[slot % colors.length];
}

/**
 * The slot of a series that is selected: the first one nobody uses, so that a color follows its
 * series for as long as it is selected. Past `SLOTS` series the colors are used again.
 */
export function freeSlot(used: number[]): number {
  for (let slot = 0; slot < SLOTS; slot++) {
    if (!used.includes(slot)) return slot;
  }
  return used.length % SLOTS;
}

/** The ink of a tick on a color: dark on the light colors (the yellow), white on the others. */
export function readableOn(hex: string): string {
  const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255);
  return 0.2126 * r + 0.7152 * g + 0.0722 * b > 0.45 ? '#111827' : '#ffffff';
}
