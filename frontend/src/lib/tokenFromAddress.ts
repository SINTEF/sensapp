import { useAuthStore } from '../stores/useAuthStore';

/**
 * A local SensApp prints a link with its token in the address fragment (`/ui/#token=…`). The
 * fragment is never sent to the server nor logged, so the token does not leave the browser: sign
 * in with it, and take it out of the address so that it is not kept in the history, bookmarked or
 * shared by mistake.
 */
export function pickUpTokenFromAddress(): void {
  const params = new URLSearchParams(window.location.hash.replace(/^#/, ''));
  const token = params.get('token');
  if (token === null) return;

  if (token.trim() !== '') {
    useAuthStore.getState().signIn(token);
  }
  params.delete('token');
  const rest = params.toString();
  window.history.replaceState(
    window.history.state,
    '',
    window.location.pathname + window.location.search + (rest ? `#${rest}` : ''),
  );
}
