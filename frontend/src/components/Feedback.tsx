/** The spinner of a table that loads. */
export function Loading() {
  return (
    <div className="flex items-center justify-center gap-2 py-6">
      <span className="loading loading-spinner loading-xs text-primary" />
      <span className="text-xs text-base-content/50">Loading...</span>
    </div>
  );
}

/** What failed, in the place of the data. */
export function ErrorAlert({ what, error }: { what: string; error: unknown }) {
  return (
    <div role="alert" className="alert alert-error">
      <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 shrink-0" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
        <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
      </svg>
      <span>Failed to load {what}: {error instanceof Error ? error.message : 'Unknown error'}</span>
    </div>
  );
}
