import { Link, Outlet } from 'react-router-dom';
import { AuthDialog } from './AuthDialog';
import { AuthStatus } from './AuthStatus';

// The files of public/, served next to the page (`BASE_URL` is `/ui/` when SensApp serves them)
const base = import.meta.env.BASE_URL;
const logo = `${base}sensapp_logo-fs8.png`;
const logoWhite = `${base}sensapp_logo_white-fs8.png`;
const logoSmall = `${base}sensapp_logo_small-fs8.png`;
const logoSmallWhite = `${base}sensapp_logo_small_white-fs8.png`;

export function Layout() {
  return (
    <div className="min-h-screen lg:h-screen flex flex-col bg-base-200 lg:overflow-hidden">
      <header className="bg-base-100 border-b border-base-300 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-3 sm:px-4 lg:px-6">
          <div className="flex items-center justify-between gap-3 h-14">
            <div className="flex items-center gap-3">
              <Link to="/" className="flex items-center hover:opacity-80 transition-opacity">
                {/* The white logo on a dark page, the small one (no name) on a narrow screen */}
                <picture>
                  <source media="(min-width: 640px) and (prefers-color-scheme: dark)" srcSet={logoWhite} />
                  <source media="(min-width: 640px)" srcSet={logo} />
                  <source media="(prefers-color-scheme: dark)" srcSet={logoSmallWhite} />
                  <img src={logoSmall} alt="SensApp" className="h-10 w-auto" />
                </picture>
              </Link>
              <div className="hidden sm:block text-xs text-base-content/40 border-l border-base-300 pl-3 ml-1">
                Sensor Data Explorer
              </div>
            </div>
            <div className="flex items-center gap-3">
              <AuthStatus />
              <a
                href="/docs"
                target="_blank"
                rel="noopener noreferrer"
                className="btn btn-ghost btn-sm gap-1.5 text-xs font-medium"
              >
                <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
                  <path fillRule="evenodd" d="M4 4a2 2 0 012-2h4.586A2 2 0 0112 2.586L15.414 6A2 2 0 0116 7.414V16a2 2 0 01-2 2H6a2 2 0 01-2-2V4z" clipRule="evenodd" />
                </svg>
                <span className="max-sm:sr-only">API Docs</span>
              </a>
            </div>
          </div>
        </div>
      </header>

      <main className="flex-1 lg:min-h-0 max-w-7xl w-full mx-auto px-3 sm:px-4 lg:px-6 py-3">
        <Outlet />
      </main>
      <AuthDialog />
    </div>
  );
}
