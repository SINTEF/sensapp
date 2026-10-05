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
