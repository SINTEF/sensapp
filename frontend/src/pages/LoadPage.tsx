import { Fragment, useState } from 'react';
import type { KeyboardEvent, ReactNode } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { CodeBlock } from '../components/CodeBlock';
import { LOAD_WAYS, sectionsFor } from '../lib/loadSnippets';
import type { LoadWay } from '../lib/loadSnippets';
import { useAuthStore } from '../stores/useAuthStore';

/** `code` between backticks is shown as code. */
function withCode(text: string): ReactNode {
  return text.split('`').map((part, index) =>
    index % 2 === 1 ? (
      <code key={index} className="font-mono text-[0.8em] bg-base-200 px-0.5 py-0.5 rounded">
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
    <div className="flex flex-col gap-3 px-1 sm:px-2 pb-8">
      <div role="tablist" aria-label="Way to load data" className="flex gap-1 sm:gap-3 border-b border-base-300" onKeyDown={handleTabKey}>
        {LOAD_WAYS.map((item) => (
          <button
            key={item.id}
            id={`load-tab-${item.id}`}
            type="button"
            role="tab"
            aria-selected={way === item.id}
            aria-controls="load-panel"
            tabIndex={way === item.id ? 0 : -1}
            className={`whitespace-nowrap px-3 sm:px-4 py-2.5 -mb-px border-b-2 text-sm font-medium transition-colors cursor-pointer ${
              way === item.id
                ? 'border-primary text-base-content'
                : 'border-transparent text-base-content/55 hover:text-base-content hover:border-base-content/25'
            }`}
            onClick={() => choose(item.id)}
          >
            {item.label}
          </button>
        ))}
      </div>

      <div id="load-panel" role="tabpanel" aria-labelledby={`load-tab-${way}`} className="flex flex-col">
        <div className="flex flex-col divide-y divide-base-300">
          {sections.map((section) => (
            <section key={section.title} className="grid gap-x-8 gap-y-3 py-6 first:pt-4 lg:grid-cols-[minmax(0,20rem)_minmax(0,1fr)]">
              <div>
                <h2 className="text-base font-semibold">{section.title}</h2>
                <p className="text-sm text-base-content/70 mt-1 leading-relaxed">{withCode(section.description)}</p>
                {section.link && (
                  <Link to={section.link.to} className="link link-primary text-sm inline-block mt-2">
                    {section.link.label}
                  </Link>
                )}
              </div>
              <CodeBlock code={section.code} language={section.language} label={section.title} />
            </section>
          ))}
        </div>
      </div>
    </div>
  );
}

export default LoadPage;
