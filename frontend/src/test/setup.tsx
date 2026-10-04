import '@testing-library/jest-dom/vitest';

// echarts needs a canvas, which jsdom does not have.
vi.mock('../components/EChart', () => ({
  default: (props: { option: unknown }) => {
    const series = (props.option as { series?: unknown[] })?.series;
    return (
      <div data-testid="echarts" data-series-count={Array.isArray(series) ? series.length : 0}>
        Chart
      </div>
    );
  },
}));
