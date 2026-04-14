import { Outlet } from 'react-router-dom';
import { HealthBadge } from './HealthBadge';

export function Layout() {
  return (
    <div className="h-screen flex flex-col bg-base-200 overflow-hidden">
      <header className="bg-base-100 border-b border-base-300 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-14">
            <div className="flex items-center gap-3">
              <a href="/" className="flex items-center gap-2 hover:opacity-80 transition-opacity">
                <svg xmlns="http://www.w3.org/2000/svg" className="h-7 w-7 text-primary" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
                </svg>
                <span className="text-lg font-bold tracking-tight">SensApp</span>
              </a>
              <div className="hidden sm:block text-xs text-base-content/40 border-l border-base-300 pl-3 ml-1">
                Sensor Data Explorer
              </div>
            </div>
            <div className="flex items-center gap-3">
              <HealthBadge />
              <a
                href="/docs"
                target="_blank"
                rel="noopener noreferrer"
                className="btn btn-ghost btn-sm gap-1.5 text-xs font-medium"
              >
                <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
                  <path fillRule="evenodd" d="M4 4a2 2 0 012-2h4.586A2 2 0 0112 2.586L15.414 6A2 2 0 0116 7.414V16a2 2 0 01-2 2H6a2 2 0 01-2-2V4z" clipRule="evenodd" />
                </svg>
                API Docs
              </a>
            </div>
          </div>
        </div>
      </header>

      <main className="flex-1 min-h-0 max-w-7xl w-full mx-auto px-3 sm:px-4 lg:px-6 py-3">
        <Outlet />
      </main>
    </div>
  );
}
