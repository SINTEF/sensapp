import { useEffect } from 'react';
import { useSelectionStore } from '../stores/useSelectionStore';

export const REFRESH_MS = 60_000;

/**
 * A preset such as `1h` is the last hour, so its end is now: it moves on every minute while the
 * tab is on screen, and at once when the tab comes back. Dates that were typed do not move.
 */
export function useLiveRange() {
  const relativeRange = useSelectionStore((state) => state.relativeRange);

  useEffect(() => {
    if (!relativeRange) return;
    const refresh = () => {
      if (!document.hidden) useSelectionStore.getState().setRelativeRange(relativeRange);
    };
    const timer = setInterval(refresh, REFRESH_MS);
    document.addEventListener('visibilitychange', refresh);
    return () => {
      clearInterval(timer);
      document.removeEventListener('visibilitychange', refresh);
    };
  }, [relativeRange]);
}
