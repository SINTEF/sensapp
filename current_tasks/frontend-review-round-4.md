# Frontend: fourth review round (5 October 2026)

Branch: `frontend-good-enough`.

## Time: one window, one place

The problem: the chart had two ways to choose what it shows. The time range control (presets, dates) decided what is
fetched, and the echarts zoom slider, the wheel and the toolbox zoomed into what was already fetched. Zooming
showed the same coarse points bigger (a day averaged per minute, zoomed on 15 minutes, is 15 points), the slider
showed the very thing the chart shows, and nothing said how to get back.

Decision: **the time window of the store is the only window.** The chart draws exactly that window.

- Drag on the chart (a horizontal brush, native echarts) = zoom: it sets the window, the data is fetched again at
  the step that fits it (a few seconds of raw samples when zoomed in). No slider, no inside zoom, no wheel (the
  wheel scrolls the page on small screens).
- Navigation below the chart, where the thumb and the eyes are after looking at it: `‹` `›` pan by half a window,
  `−` zooms out (twice the window, or the next preset for a live range), `↶` goes back to the window before (a
  history of the last 20 windows made by the user, not by the clock), then the presets, the dates, the step and the
  aggregation. The chart header keeps what is about the drawing (style, log).
- While the new data loads the chart keeps the old points on the new axis (an instant, coarse zoom that
  sharpens), with a thin progress bar on top instead of a line of text that moves the chart.
- Not done: an overview of the whole life of the series in place of the slider (`/series/{uuid}/availability`
  could feed it). It would be a third thing to look at; written in `ideas/`.

## Smaller

- Header: same horizontal padding as the cards so the logo lines up with them. The image has no padding of its
  own: what looks like space is the pale dish.
- Series list sorted by the first label column, then the next ones, numbers in natural order (`node-2` before
  `node-10`); a click on a column header sorts by it. The server pages by creation order, so the sort is of the
  page on screen.
- The placeholder of the selector is made of the labels of the first series.
- The checkbox takes the color of the series (no more dot).
- Labels that are the same on every series are said once above the table, not in a column (Influx org and bucket,
  which the importer adds on purpose: they are part of the identity of a series).
- More than 8 series: 24 colors, the validated 8 then 16 chosen by farthest-point sampling in OKLab under normal
  and color-blind vision; hovering a row highlights its series in the chart, because past 8 color alone does not
  say which is which.

## Steps

1. [ ] Header alignment
2. [ ] Time: store (history, pan, zoom out), the bar below the chart, brush zoom, progress bar
3. [ ] Series list: sort, placeholder, colored checkbox, shared labels
4. [ ] 24 colors, hover highlight
5. [ ] Docs, live check, done/
