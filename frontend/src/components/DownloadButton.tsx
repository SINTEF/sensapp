import { lazy, Suspense, useState } from 'react';
import { useSelectionStore } from '../stores/useSelectionStore';

const DownloadDialog = lazy(() => import('./DownloadDialog'));

/** Saves the selected series as files. */
export function DownloadButton() {
  const [open, setOpen] = useState(false);
  const nothingSelected = useSelectionStore((state) => state.selectedSeries.length === 0);

  return (
    <>
      <button
        type="button"
        className="btn btn-ghost btn-sm gap-1.5 text-xs font-medium"
        disabled={nothingSelected}
        title={nothingSelected ? 'Select series to download them' : undefined}
        onClick={() => setOpen(true)}
      >
        <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d="M12 3v12M7 10l5 5 5-5M5 21h14" />
        </svg>
        <span className="max-sm:sr-only">Download</span>
      </button>
      {open && (
        <Suspense fallback={null}>
          <DownloadDialog onClose={() => setOpen(false)} />
        </Suspense>
      )}
    </>
  );
}
