import { lazy, Suspense, useState } from 'react';

// highlight.js and the snippets are loaded when the dialog is first opened
const CodeDialog = lazy(() => import('./CodeDialog'));

/** Opens the code that loads what the explorer shows. */
export function CodeButton() {
  const [open, setOpen] = useState(false);

  return (
    <>
      <button
        type="button"
        className="btn btn-ghost btn-sm gap-1.5 text-xs font-medium"
        onClick={() => setOpen(true)}
      >
        <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d="M16 18l6-6-6-6M8 6l-6 6 6 6" />
        </svg>
        <span className="max-sm:sr-only">Code</span>
      </button>
      {open && (
        <Suspense fallback={null}>
          <CodeDialog onClose={() => setOpen(false)} />
        </Suspense>
      )}
    </>
  );
}
