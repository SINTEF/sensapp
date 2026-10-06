import type { ReactNode } from 'react';

/** A card of the explorer: a title with its controls on one row, then the content. */
export function Panel({ title, controls, children }: { title: ReactNode; controls?: ReactNode; children: ReactNode }) {
  return (
    <section className="bg-base-100 rounded-lg border border-base-300 shadow-sm flex flex-col min-h-72 max-h-[70vh] lg:max-h-none lg:min-h-0">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5 px-4 py-2 border-b border-base-300 shrink-0">
        <h2 className="text-sm font-semibold whitespace-nowrap">{title}</h2>
        {controls}
      </div>
      <div className="p-3 pt-2 flex-1 min-h-0 flex flex-col">{children}</div>
    </section>
  );
}
