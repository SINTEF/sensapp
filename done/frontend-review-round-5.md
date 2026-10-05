# Frontend: fifth review round (5 October 2026)

Branch: `frontend-good-enough`.

- **Width.** The header and the page were capped at 1280 px (`max-w-7xl`, from the first layout): a wide window had
  margins on both sides. Both use the whole window now, and the chart is `clamp(260px, 40vh, 640px)` high instead
  of a fixed 260 px, so a tall window gives it room.
- **"Drag on the chart to zoom"** removed: the crosshair cursor and the selection that follows the pointer say it,
  and `docs/FRONTEND.md` does.
- **Controls, one look in both themes.** daisyUI draws outline buttons and badges in the full text color, which is
  white frames on the dark theme. `index.css` has, outside the layers so that it wins: a border that is 18 % of the
  text color (32 % on hover) for buttons and fields, `btn-quiet` (a wash on hover, tinted with the primary color
  when it is on: the active preset, `log`), `chip` (a wash, no frame) for labels and dimensions, soft type badges,
  a hover wash that is not darker than the card, and thin scrollbars that are not brighter than the page.
- Dimensions of the metrics list wrap in a wider cell (the name takes 40 %, not everything).

Looked at in a browser, light and dark, at 1280 and 1600 px wide. Not looked at: the phone width, which these
changes do not touch.

## Vertical space (same day)

- The controls of the chart (back, earlier, zoom out, later, presets, the two dates on one line, step, aggregation)
  moved from a bar below the chart into its header, next to the style and the scale. This undoes the "below the
  chart" of the fourth round, on purpose: the room is worth more to the chart.
- `Panel` (`components/Panel.tsx`): the title and the controls of the Metrics and Series cards share one row. The
  series count is there too ("3 of 12 series", "on this page" with a pager) and the footer of the list is gone;
  "3 selected" went, `Clear (3)` says it.
- A radio button on each metric. When the page opens on a metric (the address) the list scrolls to it, once; a
  click does not move the list.

## Spacing, hover, title (same day)

- The page had 24 px at the sides (`lg:px-6`) and 12 px between the cards and below: it is 12 px (`p-3`, `gap-3`)
  all around now, the header included, so the logo lines up with the cards.
- Hover: the ghost buttons (API Docs, Clear, Sign out) had daisyUI's hover, the bar buttons their own. Both use
  the same wash now (`--wash`); the quiet ones also darken their border. "Sign in" was an outline in the primary
  color that filled on hover: it is a quiet button too.
- "Sensor Data Explorer" is "Data Explorer", in capitals with wide tracking, in Sora (variable, latin only,
  34 kB, `@fontsource-variable/sora`), the one place it is used (`font-display`).
