import '@testing-library/jest-dom/vitest';

// Mock echarts-for-react globally since it requires canvas in jsdom
vi.mock('echarts-for-react', () => ({
  default: (props: { option: unknown }) => {
    const seriesCount = Array.isArray((props.option as Record<string, unknown>)?.series)
      ? ((props.option as Record<string, unknown>).series as unknown[]).length
      : 0;
    return (
      <div data-testid="echarts" data-series-count={seriesCount}>
        Chart
      </div>
    );
  },
}));
