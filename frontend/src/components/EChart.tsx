import { useEffect, useRef } from 'react';
import * as echarts from 'echarts/core';
import { BarChart, LineChart } from 'echarts/charts';
import {
  BrushComponent,
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
  BrushComponent,
  GridComponent,
  ToolboxComponent,
  TooltipComponent,
  CanvasRenderer,
]);

/**
 * Minimal echarts wrapper: one instance per mount, resized with its container. A new option is merged
 * into what is drawn, so that a series that is added or removed comes and goes with an animation
 * while the others stay. A drag on the chart is a window: `onBrush` gets its two ends, in ms.
 */
export default function EChart({
  option,
  onBrush,
}: {
  option: echarts.EChartsCoreOption;
  onBrush?: (fromMs: number, toMs: number) => void;
}) {
  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts | null>(null);
  const dark = usePrefersDark();

  const onBrushRef = useRef(onBrush);
  useEffect(() => {
    onBrushRef.current = onBrush;
  });

  // A new instance when the theme changes: echarts cannot switch it on a live one
  useEffect(() => {
    const element = container.current;
    if (!element) return;
    const instance = echarts.init(element, dark ? 'dark' : undefined);
    chart.current = instance;
    // A drag draws a window, which is asked for and then wiped: the chart shows the new one
    instance.on('brushEnd', (event) => {
      const range = (event as { areas?: Array<{ coordRange?: number[] }> }).areas?.[0]?.coordRange;
      if (range && range.length === 2) onBrushRef.current?.(range[0], range[1]);
      instance.dispatchAction({ type: 'brush', areas: [] });
    });
    const observer = new ResizeObserver(() => instance.resize());
    observer.observe(element);
    return () => {
      observer.disconnect();
      instance.dispose();
      chart.current = null;
    };
  }, [dark]);

  useEffect(() => {
    const instance = chart.current;
    if (!instance) return;
    // The theme brings a background of its own: the card has one already
    instance.setOption(
      { backgroundColor: 'transparent', ...option },
      // What a merge would keep of a series or an axis that is gone
      { replaceMerge: ['series', 'yAxis'] },
    );
    // Dragging selects a window, with no button to press first
    instance.dispatchAction({
      type: 'takeGlobalCursor',
      key: 'brush',
      brushOption: { brushType: 'lineX', brushMode: 'single' },
    });
  }, [option, dark]);

  return <div ref={container} className="h-full w-full" />;
}
