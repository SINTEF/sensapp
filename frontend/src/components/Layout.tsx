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
          {/*
            The row covers the border of the header, so the line under the current tab sits on it.
            On a narrow screen it wraps: the logo and the buttons, then the tabs on a row of their own.
          */}
          <div className="flex flex-wrap md:flex-nowrap items-center gap-x-2 md:gap-x-3 -mb-px">
            <Link to="/" className="order-1 flex items-center shrink-0 h-12 md:h-14 hover:opacity-80 transition-opacity">
              {/* The white logo on a dark page, the small one (no name) on a narrow screen */}
              <picture>
                <source media="(min-width: 640px) and (prefers-color-scheme: dark)" srcSet={logoWhite} />
                <source media="(min-width: 640px)" srcSet={logo} />
                <source media="(prefers-color-scheme: dark)" srcSet={logoSmallWhite} />
                <img src={logoSmall} alt="SensApp" className="h-10 w-auto max-w-none" />
              </picture>
            </Link>
            <span aria-hidden="true" className="order-2 max-md:hidden h-6 w-px bg-base-300 shrink-0" />
            <nav
              aria-label="Pages"
              className="order-3 max-md:-mx-3 max-md:w-[calc(100%+1.5rem)] max-md:border-t max-md:border-base-300 flex items-stretch h-10 md:h-14 md:gap-1 min-w-0"
            >
              {TABS.map((tab) => (
                <NavLink
                  key={tab.to}
                  to={tab.to}
                  end={tab.end}
                  className={({ isActive }) =>
                    `flex items-center justify-center max-md:flex-1 whitespace-nowrap md:px-3 pt-1 border-b-2 font-display text-[0.65rem] md:text-[0.7rem] font-semibold uppercase tracking-[0.06em] md:tracking-[0.12em] lg:tracking-[0.2em] transition-colors ${
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
            <div className="max-md:order-2 md:order-4 ml-auto flex items-center gap-1 md:gap-3 shrink-0">
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
