import { create } from 'zustand';

interface SelectedSeries {
  uuid: string;
  name: string;
  labels: Record<string, string>;
  type: string;
}

interface SelectionState {
  selectedMetric: string | null;
  selectedSeries: SelectedSeries[];
  timeRange: {
    start: string;
    end: string;
  };
  labelFilter: string;
  setSelectedMetric: (metric: string | null) => void;
  toggleSeries: (series: SelectedSeries) => void;
  clearSelectedSeries: () => void;
  setTimeRange: (start: string, end: string) => void;
  setLabelFilter: (filter: string) => void;
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
  labelFilter: '',

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
      return {
        selectedSeries: [...state.selectedSeries, series],
      };
    }),

  clearSelectedSeries: () => set({ selectedSeries: [] }),

  setTimeRange: (start, end) => set({ timeRange: { start, end } }),

  setLabelFilter: (filter) => set({ labelFilter: filter }),
}));
