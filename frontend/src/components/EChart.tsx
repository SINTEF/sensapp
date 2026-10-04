import { useEffect, useRef } from 'react';
import * as echarts from 'echarts/core';
import { LineChart } from 'echarts/charts';
import {
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  ToolboxComponent,
  TooltipComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';

// Only what the time series chart needs: the full echarts bundle is three times bigger.
echarts.use([
  LineChart,
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  ToolboxComponent,
  TooltipComponent,
  CanvasRenderer,
]);

/** Minimal echarts wrapper: one instance per mount, resized with its container. */
export default function EChart({ option }: { option: echarts.EChartsCoreOption }) {
  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts | null>(null);

  useEffect(() => {
    const element = container.current;
    if (!element) return;
    const instance = echarts.init(element);
    chart.current = instance;
    const observer = new ResizeObserver(() => instance.resize());
    observer.observe(element);
    return () => {
      observer.disconnect();
      instance.dispose();
      chart.current = null;
    };
  }, []);

  useEffect(() => {
    chart.current?.setOption(option, true);
  }, [option]);

  return <div ref={container} className="h-full w-full" />;
}
