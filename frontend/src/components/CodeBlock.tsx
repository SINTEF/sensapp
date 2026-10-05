import { useEffect, useMemo, useRef, useState } from 'react';
import { copyText } from '../lib/copyText';
import { highlight } from '../lib/highlight';

/** Code on a dark ground, in both themes, with a button that copies it. */
export function CodeBlock({ code, language, label }: { code: string; language: Parameters<typeof highlight>[1]; label: string }) {
  const [copied, setCopied] = useState<boolean | null>(null);
  const html = useMemo(() => highlight(code, language), [code, language]);

  const timer = useRef<number>(undefined);
  useEffect(() => () => window.clearTimeout(timer.current), []);

  async function handleCopy() {
    setCopied(await copyText(code));
    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(() => setCopied(null), 2000);
  }

  return (
    <div className="relative">
      <pre className="code-block rounded-lg px-4 py-3 pr-24" aria-label={label} tabIndex={0}>
        <code dangerouslySetInnerHTML={{ __html: html }} />
      </pre>
      <button
        type="button"
        className="btn btn-quiet btn-xs absolute top-2 right-2 min-w-16 bg-base-100/90"
        aria-label={`Copy ${label}`}
        onClick={() => void handleCopy()}
      >
        {copied === true ? 'Copied' : copied === false ? 'Failed' : 'Copy'}
      </button>
      <span className="sr-only" role="status">
        {copied === true ? 'Copied to the clipboard' : copied === false ? 'Could not copy' : ''}
      </span>
    </div>
  );
}
