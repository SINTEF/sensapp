import { useEffect, useMemo, useRef, useState } from 'react';
import type { KeyboardEvent } from 'react';
import hljs from 'highlight.js/lib/core';
import bash from 'highlight.js/lib/languages/bash';
import python from 'highlight.js/lib/languages/python';
import { resolveStep } from '../lib/chartStep';
import { copyText } from '../lib/copyText';
import { SNIPPET_LANGUAGES, snippetFor } from '../lib/snippets';
import type { SnippetLanguage } from '../lib/snippets';
import { useAuthStore } from '../stores/useAuthStore';
import { useSelectionStore } from '../stores/useSelectionStore';

hljs.registerLanguage('python', python);
hljs.registerLanguage('bash', bash);

const LABELS: Record<SnippetLanguage, string> = { python: 'Python', curl: 'curl' };
const HIGHLIGHT_AS: Record<SnippetLanguage, string> = { python: 'python', curl: 'bash' };

/** Code that loads what the explorer shows: the Python SDK, or curl. */
export default function CodeDialog({ onClose }: { onClose: () => void }) {
  const [language, setLanguage] = useState<SnippetLanguage>('python');
  const [copied, setCopied] = useState<boolean | null>(null);
  const { selectedMetric, selectedSeries, timeRange, relativeRange, step, aggregation } = useSelectionStore();
  const { token, authRequired } = useAuthStore();

  const code = useMemo(
    () =>
      snippetFor(language, {
        baseUrl: import.meta.env.VITE_SENSAPP_API_URL || window.location.origin,
        authenticated: token !== null || authRequired,
        metric: selectedMetric,
        series: selectedSeries,
        timeRange,
        relativeRange,
        step: resolveStep(step, timeRange.start, timeRange.end),
        aggregation,
      }),
    [language, selectedMetric, selectedSeries, timeRange, relativeRange, step, aggregation, token, authRequired],
  );
  // highlight.js escapes the text it is given: what it returns is safe to put in the page
  const html = useMemo(() => hljs.highlight(code, { language: HIGHLIGHT_AS[language] }).value, [code, language]);

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
          <pre className="code-block rounded-lg px-4 py-3 overflow-auto min-h-0 max-h-[60vh]">
            <code dangerouslySetInnerHTML={{ __html: html }} />
          </pre>
          {nothingSelected && (
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
