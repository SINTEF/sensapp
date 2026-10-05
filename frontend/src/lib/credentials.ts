import type { TokenInfo } from '../stores/useAuthStore';

/** What makes a token that can make tokens. It reads nothing, and the endpoint never makes another one. */
export const ADMIN_TOKEN_COMMAND = 'sensapp generate-token me --scope admin';

/** The names of a comma separated list, trimmed, without blanks nor repeats. */
export function parseSensorNames(text: string): string[] {
  return [...new Set(text.split(',').map((name) => name.trim()).filter(Boolean))];
}

/** The scopes a token says it has. A token without the claim has the default one of the server. */
export function tokenScopes(info: TokenInfo | null): string[] {
  return info?.scope === undefined ? ['read', 'write'] : info.scope.split(/\s+/).filter(Boolean);
}
