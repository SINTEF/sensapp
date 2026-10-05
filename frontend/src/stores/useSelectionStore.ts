import { create } from 'zustand';
import type { Aggregation } from '../lib/chartStep';
import type { ChartStyle } from '../lib/chartOption';
import { freeSlot } from '../lib/palette';
import { DEFAULT_RANGE, rangeFor } from '../lib/timeRange';

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
  timeRange: {
    start: string;
    end: string;
  };
  /** The preset the range comes from (`24h`), `null` once dates were typed */
  relativeRange: string | null;
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
  setTimeRange: (start: string, end: string) => void;
  /** A preset: the range ending now */
  setRelativeRange: (label: string) => void;
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

export const useSelectionStore = create<SelectionState>((set) => ({
  selectedMetric: null,
  selectedSeries: [],
  timeRange: defaultTimeRange(),
  relativeRange: DEFAULT_RANGE,
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
    set({ timeRange: { start, end }, relativeRange: null }),

  setRelativeRange: (label) => {
    const timeRange = rangeFor(label);
    if (timeRange) set({ timeRange, relativeRange: label });
  },

  setChartStyle: (chartStyle) => set({ chartStyle }),

  setLogScale: (logScale) => set({ logScale }),

  setLabelFilter: (filter) => set({ labelFilter: filter }),

  setStep: (step) => set({ step }),

  setAggregation: (aggregation) => set({ aggregation }),
}));
