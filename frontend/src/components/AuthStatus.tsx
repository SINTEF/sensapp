import { useQueryClient } from '@tanstack/react-query';
import { describeToken, useAuthStore } from '../stores/useAuthStore';

/** Who is signed in, with the way out; or a way in once the server asked for a token. */
export function AuthStatus() {
  const { token, authRequired, signOut, openDialog } = useAuthStore();
  const queryClient = useQueryClient();

  if (token) {
    const info = describeToken(token);
    const details = [
      info?.scope && `scope: ${info.scope}`,
      info?.expires && `expires: ${info.expires.toLocaleString()}`,
    ].filter(Boolean);

    function handleSignOut() {
      signOut();
      // Nothing of a signed-out session stays in memory, and what is on screen asks again
      void queryClient.resetQueries();
    }

    return (
      <div className="flex items-center gap-1.5" title={details.join(' · ') || undefined}>
        <span className="text-xs font-medium opacity-70 truncate max-sm:max-w-20">{info?.subject ?? 'token'}</span>
        <button className="btn btn-ghost btn-sm text-xs font-medium" onClick={handleSignOut}>
          Sign out
        </button>
      </div>
    );
  }

  if (authRequired) {
    return (
      <button className="btn btn-quiet btn-sm text-xs font-medium" onClick={openDialog}>
        Sign in
      </button>
    );
  }

  return null;
}
