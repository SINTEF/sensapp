import { create } from 'zustand';
import type { Aggregation } from '../lib/chartStep';
import type { ChartStyle } from '../lib/chartOption';
import { freeSlot } from '../lib/palette';
import { DEFAULT_RANGE, nextPreset, panned, rangeFor, zoomedOut } from '../lib/timeRange';
import type { TimeRange } from '../lib/timeRange';

/** What the server says of a series. */
export interface SeriesInfo {
  uuid: string;
  name: string;
  labels: Record<string, string>;
  type: string;
}

/** A series that is shown, with the color slot it keeps for as long as it is. */
export interface SelectedSeries extends SeriesInfo {
  slot: number;
}

/** A time window, and the preset it comes from if it has one. */
export interface WindowState {
  timeRange: TimeRange;
  relativeRange: string | null;
}

const HISTORY_SIZE = 20;

/** The series with a slot each, the ones that have one keeping it. */
export function withSlots(series: SeriesInfo[], selected: SelectedSeries[] = []): SelectedSeries[] {
  const result = [...selected];
  for (const info of series) {
    if (result.some((s) => s.uuid === info.uuid)) continue;
    result.push({ ...info, slot: freeSlot(result.map((s) => s.slot)) });
  }
  return result;
}

interface SelectionState {
  selectedMetric: string | null;
  selectedSeries: SelectedSeries[];
  timeRange: TimeRange;
  /** The preset the range comes from (`24h`), `null` once dates were typed */
  relativeRange: string | null;
  /** The windows the user left, the last one first: what Back goes to. The clock moving a preset is not one. */
  rangeHistory: WindowState[];
  chartStyle: ChartStyle;
  logScale: boolean;
  /** `auto` (the range decides), `raw`, or a duration such as `5m` */
  step: string;
  aggregation: Aggregation;
  labelFilter: string;
  setSelectedMetric: (metric: string | null) => void;
  toggleSeries: (series: SeriesInfo) => void;
  /** Shows these series too */
  selectSeries: (series: SeriesInfo[]) => void;
  clearSelectedSeries: () => void;
  /** A window of dates: typed, or brushed on the chart */
  setTimeRange: (start: string, end: string) => void;
  /** A preset: the range ending now */
  setRelativeRange: (label: string) => void;
  /** The end of a preset moves with the clock. Nothing is kept of it in the history. */
  refreshRelativeRange: () => void;
  /** The window half a window earlier or later */
  panRange: (direction: -1 | 1) => void;
  /** Twice the window, a live preset going to the next one */
  zoomOutRange: () => void;
  /** Back to the window before the last change of the user */
  undoRange: () => void;
  setChartStyle: (style: ChartStyle) => void;
  setLogScale: (log: boolean) => void;
  setLabelFilter: (filter: string) => void;
  setStep: (step: string) => void;
  setAggregation: (aggregation: Aggregation) => void;
}

function defaultTimeRange() {
  const end = new Date();
  const start = new Date(end.getTime() - 60 * 60 * 1000); // 1 hour ago
  return {
    start: start.toISOString(),
    end: end.toISOString(),
  };
}

function remember(state: SelectionState): WindowState[] {
  return [{ timeRange: state.timeRange, relativeRange: state.relativeRange }, ...state.rangeHistory].slice(0, HISTORY_SIZE);
}

export const useSelectionStore = create<SelectionState>((set) => ({
  selectedMetric: null,
  selectedSeries: [],
  timeRange: defaultTimeRange(),
  relativeRange: DEFAULT_RANGE,
  rangeHistory: [],
  chartStyle: 'line',
  logScale: false,
  labelFilter: '',
  step: 'auto',
  aggregation: 'avg',

  setSelectedMetric: (metric) =>
    set({ selectedMetric: metric, selectedSeries: [] }),

  toggleSeries: (series) =>
    set((state) => {
      const exists = state.selectedSeries.find((s) => s.uuid === series.uuid);
      if (exists) {
        return {
          selectedSeries: state.selectedSeries.filter(
            (s) => s.uuid !== series.uuid
          ),
        };
      }
      return { selectedSeries: withSlots([series], state.selectedSeries) };
    }),

  selectSeries: (series) =>
    set((state) => ({ selectedSeries: withSlots(series, state.selectedSeries) })),

  clearSelectedSeries: () => set({ selectedSeries: [] }),

  setTimeRange: (start, end) =>
    set((state) => ({
      timeRange: { start, end },
      relativeRange: null,
      rangeHistory: remember(state),
    })),

  setRelativeRange: (label) =>
    set((state) => {
      const timeRange = rangeFor(label);
      if (!timeRange) return {};
      // The same preset again is a refresh: there is nothing to go back to
      const same = state.relativeRange === label;
      return { timeRange, relativeRange: label, ...(!same && { rangeHistory: remember(state) }) };
    }),

  refreshRelativeRange: () =>
    set((state) => {
      const timeRange = state.relativeRange ? rangeFor(state.relativeRange) : undefined;
      return timeRange ? { timeRange } : {};
    }),

  panRange: (direction) =>
    set((state) => ({
      timeRange: panned(state.timeRange, direction),
      relativeRange: null,
      rangeHistory: remember(state),
    })),

  zoomOutRange: () =>
    set((state) => {
      if (state.relativeRange) {
        const label = nextPreset(state.relativeRange);
        const timeRange = rangeFor(label);
        return label === state.relativeRange || !timeRange
          ? {}
          : { timeRange, relativeRange: label, rangeHistory: remember(state) };
      }
      return { timeRange: zoomedOut(state.timeRange), rangeHistory: remember(state) };
    }),

  undoRange: () =>
    set((state) => {
      const [previous, ...rest] = state.rangeHistory;
      if (!previous) return {};
      // A preset is the last hour again, as of now
      const timeRange = (previous.relativeRange && rangeFor(previous.relativeRange)) || previous.timeRange;
      return { timeRange, relativeRange: previous.relativeRange, rangeHistory: rest };
    }),

  setChartStyle: (chartStyle) => set({ chartStyle }),

  setLogScale: (logScale) => set({ logScale }),

  setLabelFilter: (filter) => set({ labelFilter: filter }),

  setStep: (step) => set({ step }),

  setAggregation: (aggregation) => set({ aggregation }),
}));
