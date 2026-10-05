import { useEffect, useRef, useState } from 'react';
import { useQueries } from '@tanstack/react-query';
import { useSearchParams } from 'react-router-dom';
import { getSeriesData } from '../client';
import { ApiError, unwrap } from '../api/clientConfig';
import { fromSearchParams, toSearchParams } from '../lib/urlState';
import { rangeFor } from '../lib/timeRange';
import { useSelectionStore } from '../stores/useSelectionStore';
import { withSlots } from '../stores/useSelectionStore';
import type { SeriesInfo } from '../stores/useSelectionStore';

/** The type of a series, from the value its first record carries. */
function typeOf(record: Record<string, unknown>): string {
  if ('vb' in record) return 'boolean';
  if ('vs' in record) return 'string';
  if ('v' in record) return 'float';
  return 'other';
}

/**
 * What the chart needs to know of a series that an address names by its uuid: its first sample
 * carries the name and the labels. `null` for a series that is gone.
 */
async function probeSeries(uuid: string): Promise<SeriesInfo | null> {
  try {
    const result = await getSeriesData({
      path: { series_uuid: uuid },
      query: { format: 'senml', limit: 1 },
    });
    const first = (unwrap(result) as unknown as Array<Record<string, unknown>>)[0];
    if (!first) return null;
    return {
      uuid,
      name: typeof first._name === 'string' ? first._name : uuid,
      labels: (first._labels ?? {}) as Record<string, string>,
      type: typeOf(first),
    };
  } catch (error) {
    // Asking for a token is the job of the query cache, anything else is a series to forget
    if (error instanceof ApiError && !error.isAuthError) return null;
    throw error;
  }
}

/**
 * The explorer in the address: what the address says is shown when the page opens, then the
 * address follows what is shown (it replaces the history entry, a click is not a page).
 */
export function useUrlState() {
  const [params, setParams] = useSearchParams();
  const [initial] = useState(() => fromSearchParams(params));

  const setParamsRef = useRef(setParams);
  useEffect(() => {
    setParamsRef.current = setParams;
  });

  // Everything but the series is known at once
  useEffect(() => {
    const range = initial.relativeRange ? rangeFor(initial.relativeRange) : initial.timeRange;
    useSelectionStore.setState({
      ...(initial.metric && { selectedMetric: initial.metric }),
      ...(initial.relativeRange !== undefined && { relativeRange: initial.relativeRange }),
      ...(range && { timeRange: range }),
      ...(initial.step && { step: initial.step }),
      ...(initial.aggregation && { aggregation: initial.aggregation }),
      ...(initial.chartStyle && { chartStyle: initial.chartStyle }),
      ...(initial.logScale && { logScale: true }),
    });
  }, [initial]);

  // The series need a request each. It is a query: a token that is asked for is then used.
  const probes = useQueries({
    queries: (initial.series ?? []).map((uuid) => ({
      queryKey: ['seriesProbe', uuid],
      queryFn: () => probeSeries(uuid),
      staleTime: Infinity,
    })),
  });
  const settled = probes.every(
    (probe) => probe.isSuccess || (probe.isError && !(probe.error instanceof ApiError && probe.error.isAuthError)),
  );

  const probesRef = useRef(probes);
  useEffect(() => {
    probesRef.current = probes;
  });

  // Once the series are known: show them, and from then on the address follows the explorer
  useEffect(() => {
    if (!settled) return;
    const series = probesRef.current.flatMap((probe) => (probe.data ? [probe.data] : []));
    if (series.length > 0) useSelectionStore.setState({ selectedSeries: withSlots(series) });

    const write = (state: ReturnType<typeof useSelectionStore.getState>) =>
      setParamsRef.current(
        toSearchParams({
          metric: state.selectedMetric,
          series: state.selectedSeries.map((s) => s.uuid),
          relativeRange: state.relativeRange,
          timeRange: state.timeRange,
          step: state.step,
          aggregation: state.aggregation,
          chartStyle: state.chartStyle,
          logScale: state.logScale,
        }),
        { replace: true },
      );
    write(useSelectionStore.getState());
    return useSelectionStore.subscribe(write);
  }, [settled]);
}
