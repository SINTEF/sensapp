/**
 * The colors of the series, in the order they are given, one step of each for each theme.
 *
 * The first `DISTINCT_COLORS` are the eight hues of the dataviz skill's palette, validated with its
 * `validate_palette.js` (color-blind distance 8.4 or more between neighbours, 19 for normal vision).
 * The 16 after them are not a palette but the answer to "more than 8 series without repeating a color":
 * each was picked, in OKLab, as the farthest from the ones before under normal, protan and deutan
 * vision (docs/FRONTEND.md has the numbers). The ninth to eleventh are 11 (light) and 10 (dark) or more
 * apart from the others; from the twelfth it goes down to 5: past eight colors alone does not say
 * which series is which, hovering a row of the list highlights it in the chart for that.
 * The dark steps are lighter than the skill's band for the same reason: there is no room in it.
 */
export const SERIES_COLORS = {
  light: [
    '#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948',
    '#58aefb', '#803a5b', '#ac5291', '#3d94e0', '#495e0d', '#5bc971', '#02aec6', '#c93a63',
    '#324fcf', '#6b4988', '#1e86fe', '#1987bb', '#2bc4d1', '#b86e94', '#6f51ac', '#993f43',
  ],
  dark: [
    '#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300', '#9085e9', '#e66767',
    '#65d8e3', '#fab72a', '#5ae297', '#b736a0', '#41b7c7', '#226dc9', '#70bffe', '#fa7ba7',
    '#e6a05a', '#b072b5', '#c03978', '#5cacea', '#e46894', '#c74a49', '#3a70ee', '#4ac777',
  ],
};

/** The colors that are clearly apart (the validated ones). */
export const DISTINCT_COLORS = 8;

export const SLOTS = SERIES_COLORS.light.length;

export function seriesColor(slot: number, dark: boolean): string {
  const colors = dark ? SERIES_COLORS.dark : SERIES_COLORS.light;
  return colors[slot % colors.length];
}

/**
 * The slot of a series that is selected: the first one nobody uses, so that a color follows its
 * series for as long as it is selected. Past `SLOTS` (24) series the colors are used again: nobody
 * reads a chart of 25 lines by color.
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
