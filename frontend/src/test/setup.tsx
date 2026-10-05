import '@testing-library/jest-dom/vitest';

// echarts needs a canvas, which jsdom does not have.
vi.mock('../components/EChart', () => ({
  default: (props: { option: unknown; onBrush?: (fromMs: number, toMs: number) => void }) => {
    const series = (props.option as { series?: unknown[] })?.series;
    return (
      <div
        data-testid="echarts"
        data-series-count={Array.isArray(series) ? series.length : 0}
        data-option={JSON.stringify(props.option)}
      >
        Chart
        {/* A drag on the chart, from 00:10 to 00:20 on 2026-10-04 */}
        <button
          data-testid="echarts-brush"
          onClick={() => props.onBrush?.(Date.parse('2026-10-04T00:10:00.400Z'), Date.parse('2026-10-04T00:20:00.600Z'))}
        />
      </div>
    );
  },
}));
