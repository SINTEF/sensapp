import { useEffect, useRef } from 'react';
import * as echarts from 'echarts/core';
import { BarChart, LineChart } from 'echarts/charts';
import {
  DataZoomComponent,
  GridComponent,
  ToolboxComponent,
  TooltipComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import { usePrefersDark } from '../lib/usePrefersDark';

// Only what the time series chart needs: the full echarts bundle is three times bigger.
echarts.use([
  BarChart,
  LineChart,
  DataZoomComponent,
  GridComponent,
  ToolboxComponent,
  TooltipComponent,
  CanvasRenderer,
]);

/**
 * Minimal echarts wrapper: one instance per mount, resized with its container. A new option is merged
 * into what is drawn, so that a series that is added or removed comes and goes with an animation
 * while the others stay (and the zoom with them). `resetZoomKey` is what the zoom belongs to.
 */
export default function EChart({
  option,
  resetZoomKey,
}: {
  option: echarts.EChartsCoreOption;
  resetZoomKey?: string;
}) {
  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts | null>(null);
  const dark = usePrefersDark();

  // A new instance when the theme changes: echarts cannot switch it on a live one
  useEffect(() => {
    const element = container.current;
    if (!element) return;
    const instance = echarts.init(element, dark ? 'dark' : undefined);
    chart.current = instance;
    const observer = new ResizeObserver(() => instance.resize());
    observer.observe(element);
    return () => {
      observer.disconnect();
      instance.dispose();
      chart.current = null;
    };
  }, [dark]);

  useEffect(() => {
    // The theme brings a background of its own: the card has one already
    chart.current?.setOption(
      { backgroundColor: 'transparent', ...option },
      // What a merge would keep of a series or an axis that is gone
      { replaceMerge: ['series', 'yAxis'] },
    );
  }, [option, dark]);

  useEffect(() => {
    chart.current?.dispatchAction({ type: 'dataZoom', start: 0, end: 100 });
  }, [resetZoomKey]);

  return <div ref={container} className="h-full w-full" />;
}
