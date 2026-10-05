import { Link, NavLink, Outlet, useLocation } from 'react-router-dom';
import { AuthDialog } from './AuthDialog';
import { AuthStatus } from './AuthStatus';
import { CodeButton } from './CodeButton';

// The files of public/, served next to the page (`BASE_URL` is `/ui/` when SensApp serves them)
const base = import.meta.env.BASE_URL;
const logo = `${base}sensapp_logo-fs8.png`;
const logoWhite = `${base}sensapp_logo_white-fs8.png`;
const logoSmall = `${base}sensapp_logo_small-fs8.png`;
const logoSmallWhite = `${base}sensapp_logo_small_white-fs8.png`;

const TABS = [
  { to: '/', label: 'Data Explorer', end: true },
  { to: '/load', label: 'Load Data', end: false },
  { to: '/credentials', label: 'Credentials', end: false },
];

export function Layout() {
  // The Code button is about what the explorer shows
  const explorer = useLocation().pathname === '/';
  return (
    <div className="min-h-screen lg:h-screen flex flex-col bg-base-200 lg:overflow-hidden">
      <header className="bg-base-100 border-b border-base-300 sticky top-0 z-50">
        <div className="px-3">
          {/* The row covers the border of the header, so the line under the current tab sits on it */}
          <div className="flex items-center justify-between gap-3 h-14 -mb-px">
            <div className="flex items-center gap-2 sm:gap-3 self-stretch min-w-0">
              <Link to="/" className="flex items-center hover:opacity-80 transition-opacity">
                {/* The white logo on a dark page, the small one (no name) on a narrow screen */}
                <picture>
                  <source media="(min-width: 640px) and (prefers-color-scheme: dark)" srcSet={logoWhite} />
                  <source media="(min-width: 640px)" srcSet={logo} />
                  <source media="(prefers-color-scheme: dark)" srcSet={logoSmallWhite} />
                  <img src={logoSmall} alt="SensApp" className="h-10 w-auto" />
                </picture>
              </Link>
              <span aria-hidden="true" className="self-center h-6 w-px bg-base-300 shrink-0" />
              {/* On a narrow screen the tabs scroll rather than push the page wider */}
              <nav aria-label="Pages" className="flex items-stretch self-stretch sm:gap-1 min-w-0 overflow-x-auto [scrollbar-width:none]">
                {TABS.map((tab) => (
                  <NavLink
                    key={tab.to}
                    to={tab.to}
                    end={tab.end}
                    className={({ isActive }) =>
                      `flex items-center whitespace-nowrap px-1.5 sm:px-3 pt-1 border-b-2 font-display text-[0.65rem] sm:text-[0.7rem] font-semibold uppercase tracking-[0.06em] sm:tracking-[0.2em] transition-colors ${
                        isActive
                          ? 'border-primary text-base-content'
                          : 'border-transparent text-base-content/45 hover:text-base-content/80'
                      }`
                    }
                  >
                    {tab.label}
                  </NavLink>
                ))}
              </nav>
            </div>
            <div className="flex items-center gap-1 sm:gap-3 shrink-0">
              {explorer && <CodeButton />}
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
              <AuthStatus />
            </div>
          </div>
        </div>
      </header>

      <main className={`flex-1 lg:min-h-0 w-full p-3 ${explorer ? '' : 'lg:overflow-y-auto'}`}>
        <Outlet />
      </main>
      <AuthDialog />
    </div>
  );
}
