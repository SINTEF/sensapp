import { useEffect, useMemo, useRef, useState } from 'react';
import type { KeyboardEvent } from 'react';
import { resolveStep } from '../lib/chartStep';
import { copyText } from '../lib/copyText';
import { highlight } from '../lib/highlight';
import { SNIPPET_LANGUAGES, snippetFor } from '../lib/snippets';
import type { SnippetLanguage } from '../lib/snippets';
import { useAuthStore } from '../stores/useAuthStore';
import { useSelectionStore } from '../stores/useSelectionStore';

const LABELS: Record<SnippetLanguage, string> = { python: 'Python', curl: 'curl' };
const HIGHLIGHT_AS = { python: 'python', curl: 'bash' } as const;

/** Code that loads what the explorer shows: the Python SDK, or curl. */
export default function CodeDialog({ onClose }: { onClose: () => void }) {
  const [language, setLanguage] = useState<SnippetLanguage>('python');
  const [copied, setCopied] = useState<boolean | null>(null);
  const { selectedMetric, selectedSeries, selector, timeRange, relativeRange, step, aggregation } = useSelectionStore();
  // A selector that is typed in the series list is what the code asks for, unless the user prefers the series that are checked
  const [bySelector, setBySelector] = useState(true);
  const typedSelector = selector.trim();
  const useSelector = typedSelector !== '' && bySelector;
  const { token, authRequired } = useAuthStore();

  const code = useMemo(
    () =>
      snippetFor(language, {
        baseUrl: import.meta.env.VITE_SENSAPP_API_URL || window.location.origin,
        authenticated: token !== null || authRequired,
        metric: selectedMetric,
        selector: useSelector ? typedSelector : undefined,
        series: selectedSeries,
        timeRange,
        relativeRange,
        step: resolveStep(step, timeRange.start, timeRange.end),
        aggregation,
      }),
    [language, selectedMetric, selectedSeries, useSelector, typedSelector, timeRange, relativeRange, step, aggregation, token, authRequired],
  );
  const html = useMemo(() => highlight(code, HIGHLIGHT_AS[language]), [code, language]);

  const timer = useRef<number>(undefined);
  useEffect(() => () => window.clearTimeout(timer.current), []);

  async function handleCopy() {
    setCopied(await copyText(code));
    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(() => setCopied(null), 2000);
  }

  // Left and right go from a tab to the other, as in any tab list
  function handleTabKey(event: KeyboardEvent) {
    if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
    event.preventDefault();
    const next = SNIPPET_LANGUAGES[(SNIPPET_LANGUAGES.indexOf(language) + 1) % SNIPPET_LANGUAGES.length];
    setLanguage(next);
    document.getElementById(`code-tab-${next}`)?.focus();
  }

  const nothingSelected = selectedSeries.length === 0;

  return (
    <div
      className="modal modal-open"
      role="dialog"
      aria-modal="true"
      aria-labelledby="code-dialog-title"
      onKeyDown={(event) => event.key === 'Escape' && onClose()}
    >
      <div className="modal-box max-w-3xl p-0 overflow-hidden flex flex-col max-h-[90vh]">
        <div className="flex items-center justify-between gap-3 px-4 py-2 border-b border-base-300">
          <h2 id="code-dialog-title" className="text-sm font-semibold">
            Code
          </h2>
          <button type="button" className="btn btn-ghost btn-xs btn-circle" aria-label="Close" onClick={onClose}>
            ✕
          </button>
        </div>

        <div className="flex items-center justify-between gap-2 px-3 pt-2">
          <div role="tablist" aria-label="Language" className="flex gap-1" onKeyDown={handleTabKey}>
            {SNIPPET_LANGUAGES.map((id) => (
              <button
                key={id}
                id={`code-tab-${id}`}
                type="button"
                role="tab"
                aria-selected={language === id}
                aria-controls="code-panel"
                tabIndex={language === id ? 0 : -1}
                autoFocus={id === 'python'}
                className={`btn btn-quiet btn-sm ${language === id ? 'btn-active' : ''}`}
                onClick={() => setLanguage(id)}
              >
                {LABELS[id]}
              </button>
            ))}
          </div>
          <button type="button" className="btn btn-quiet btn-sm min-w-24" onClick={() => void handleCopy()}>
            {copied === true ? 'Copied' : copied === false ? 'Copy failed' : 'Copy'}
          </button>
          <span className="sr-only" role="status">
            {copied === true ? 'Copied to the clipboard' : copied === false ? 'Could not copy' : ''}
          </span>
        </div>

        <div id="code-panel" role="tabpanel" aria-labelledby={`code-tab-${language}`} className="p-3 min-h-0 flex flex-col gap-2">
          {typedSelector !== '' && (
            <label className="flex items-center gap-2 text-xs cursor-pointer">
              <input
                type="checkbox"
                className="checkbox checkbox-xs checkbox-primary"
                checked={bySelector}
                onChange={(event) => setBySelector(event.target.checked)}
              />
              <span className="min-w-0">
                Read every series matching{' '}
                <code className="font-mono bg-base-200 px-1 rounded inline-block max-w-56 truncate align-bottom" title={typedSelector}>
                  {typedSelector}
                </code>
                {selectedSeries.length > 0 && ` instead of the ${selectedSeries.length} selected`}
              </span>
            </label>
          )}
          <pre className="code-block rounded-lg px-4 py-3 overflow-auto min-h-0 max-h-[60vh]">
            <code dangerouslySetInnerHTML={{ __html: html }} />
          </pre>
          {nothingSelected && !useSelector && (
            <p className="text-xs text-base-content/60">
              {selectedMetric
                ? 'No series is selected: this lists the series of the metric. Select some to get their data.'
                : 'Nothing is selected: this lists the metrics. Select series to get their data.'}
            </p>
          )}
        </div>
      </div>
      <button type="button" className="modal-backdrop" aria-label="Close" onClick={onClose} />
    </div>
  );
}
