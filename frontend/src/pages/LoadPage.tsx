import { Fragment, useState } from 'react';
import type { KeyboardEvent, ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { CodeBlock } from '../components/CodeBlock';
import { LOAD_WAYS, WRITE_TOKEN_COMMAND, sectionsFor } from '../lib/loadSnippets';
import type { LoadWay } from '../lib/loadSnippets';
import { useAuthStore } from '../stores/useAuthStore';

/** `code` between backticks is shown as code. */
function withCode(text: string): ReactNode {
  return text.split('`').map((part, index) =>
    index % 2 === 1 ? (
      <code key={index} className="font-mono text-[0.8em] bg-base-200 px-1 py-0.5 rounded">
        {part}
      </code>
    ) : (
      <Fragment key={index}>{part}</Fragment>
    ),
  );
}

function isWay(value: string | null): value is LoadWay {
  return LOAD_WAYS.some((way) => way.id === value);
}

/** How to get data into SensApp: a way for each tab, and the code to copy. */
export function LoadPage() {
  const [params, setParams] = useSearchParams();
  const asked = params.get('via');
  const [way, setWay] = useState<LoadWay>(isWay(asked) ? asked : 'python');
  const { token, authRequired } = useAuthStore();
  const authenticated = token !== null || authRequired;

  function choose(next: LoadWay) {
    setWay(next);
    // The way is in the address so that a link shares it, without a history entry for a click
    setParams(next === 'python' ? {} : { via: next }, { replace: true });
  }

  // Left and right go from a tab to the other, as in any tab list
  function handleTabKey(event: KeyboardEvent) {
    if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
    event.preventDefault();
    const index = LOAD_WAYS.findIndex((item) => item.id === way);
    const step = event.key === 'ArrowRight' ? 1 : LOAD_WAYS.length - 1;
    const next = LOAD_WAYS[(index + step) % LOAD_WAYS.length].id;
    choose(next);
    document.getElementById(`load-tab-${next}`)?.focus();
  }

  const sections = sectionsFor(way, {
    baseUrl: import.meta.env.VITE_SENSAPP_API_URL || window.location.origin,
    authenticated,
  });

  return (
    <div className="max-w-4xl mx-auto flex flex-col gap-3">
      <section className="bg-base-100 rounded-lg border border-base-300 shadow-sm">
        <div className="px-4 py-3 border-b border-base-300">
          <h1 className="text-sm font-semibold">Load data into SensApp</h1>
          <p className="text-xs text-base-content/60 mt-1">
            Pick the way that fits your data, copy the code, and run it. The address in it is the one of this server.
            Everything else SensApp reads is in the <a className="link" href="/docs" target="_blank" rel="noopener noreferrer">API docs</a>.
          </p>
        </div>

        <div role="tablist" aria-label="Way to load data" className="flex flex-wrap gap-1 px-3 pt-3" onKeyDown={handleTabKey}>
          {LOAD_WAYS.map((item) => (
            <button
              key={item.id}
              id={`load-tab-${item.id}`}
              type="button"
              role="tab"
              aria-selected={way === item.id}
              aria-controls="load-panel"
              tabIndex={way === item.id ? 0 : -1}
              className={`btn btn-quiet btn-sm ${way === item.id ? 'btn-active' : ''}`}
              onClick={() => choose(item.id)}
            >
              {item.label}
            </button>
          ))}
        </div>

        <div id="load-panel" role="tabpanel" aria-labelledby={`load-tab-${way}`} className="p-4 flex flex-col gap-6">
          {authenticated && (
            <p className="text-xs rounded-lg bg-base-200 px-3 py-2">
              This server asks for a token. Loading needs the <code className="font-mono">write</code> scope: make one with{' '}
              <code className="font-mono">{WRITE_TOKEN_COMMAND}</code>. The code reads it from <code className="font-mono">SENSAPP_TOKEN</code>.
            </p>
          )}
          {sections.map((section) => (
            <div key={section.title} className="flex flex-col gap-2">
              <div>
                <h2 className="text-sm font-semibold">{section.title}</h2>
                <p className="text-xs text-base-content/70 mt-0.5">{withCode(section.description)}</p>
              </div>
              <CodeBlock code={section.code} language={section.language} label={section.title} />
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

export default LoadPage;
