import { create } from 'zustand';
import { createJSONStorage, persist } from 'zustand/middleware';

interface AuthState {
  /** The JWT sent as `Authorization: Bearer`. Kept for this browser tab only. */
  token: string | null;
  /** Set when the server asked for a token, to offer signing in. */
  authRequired: boolean;
  dialogOpen: boolean;
  /** What the server answered to the refused request, shown in the dialog. */
  message: string | null;
  signIn: (token: string) => void;
  signOut: () => void;
  /** The server answered 401 or 403. */
  reject: (message: string) => void;
  openDialog: () => void;
  closeDialog: () => void;
}

/** What people paste: the token alone, or with the `Bearer ` prefix or quotes around it. */
export function normalizeToken(input: string): string {
  return input
    .trim()
    .replace(/^bearer\s+/i, '')
    .replace(/^["']|["']$/g, '')
    .trim();
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      token: null,
      authRequired: false,
      dialogOpen: false,
      message: null,

      signIn: (token) =>
        set({ token: normalizeToken(token), dialogOpen: false, message: null }),
      signOut: () => set({ token: null, message: null }),
      reject: (message) =>
        set({ authRequired: true, dialogOpen: true, message }),
      openDialog: () => set({ dialogOpen: true, message: null }),
      closeDialog: () => set({ dialogOpen: false }),
    }),
    {
      name: 'sensapp-auth',
      // sessionStorage: gone when the tab closes, not shared with the other tabs
      storage: createJSONStorage(() => sessionStorage),
      partialize: ({ token }) => ({ token }),
    },
  ),
);

export interface TokenInfo {
  subject?: string;
  scope?: string;
  expires?: Date;
}

/**
 * What the token says about itself, to show who is signed in. Nothing is verified here: the
 * server does that, and answers 401 when the token is wrong.
 */
export function describeToken(token: string): TokenInfo | null {
  try {
    const payload = token.split('.')[1];
    if (!payload) return null;
    const base64 = payload.replace(/-/g, '+').replace(/_/g, '/');
    const bytes = Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));
    const claims = JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>;
    return {
      subject: typeof claims.sub === 'string' ? claims.sub : undefined,
      scope: typeof claims.scope === 'string' ? claims.scope : undefined,
      expires: typeof claims.exp === 'number' ? new Date(claims.exp * 1000) : undefined,
    };
  } catch {
    return null;
  }
}
