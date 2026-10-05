import type { TokenInfo } from '../stores/useAuthStore';

/**
 * What makes a token that can make tokens. The `admin` scope reads nothing, and the endpoint never
 * makes another admin token. `read` is added for the person who signs in with it: the explorer
 * would otherwise ask for another token.
 */
export const ADMIN_TOKEN_COMMAND = 'sensapp generate-token me --scope read,admin';

/** What the form asks for. */
export interface TokenWish {
  subject: string;
  scope: string[];
  /** Names as they are: a name may hold a comma or a space */
  sensors: string[];
  durationSeconds: number;
}

/**
 * The sensors with a name added. A name is kept exactly as typed (the server matches it exactly,
 * commas and spaces included): only a blank name, or one that is there already, is not added.
 */
export function addSensor(sensors: string[], name: string): string[] {
  if (name.trim() === '' || sensors.includes(name)) return sensors;
  return [...sensors, name];
}

/** The scopes a token says it has. A token without the claim has the default one of the server. */
export function tokenScopes(info: TokenInfo | null): string[] {
  return info?.scope === undefined ? ['read', 'write'] : info.scope.split(/\s+/).filter(Boolean);
}

/** A shell word that is one word whatever it contains, left bare when it is plain. */
export function shellWord(value: string): string {
  return /^[A-Za-z0-9_@%+=:./-]+$/.test(value) ? value : `'${value.replace(/'/g, `'\\''`)}'`;
}

/**
 * The command that makes what the form describes, for `sensapp generate-token`: the way to make an
 * admin token, and to make any token where only the command line reaches the secret. Each sensor
 * is a `--sensor` of its own, as a name may hold a comma.
 */
export function tokenCommand(wish: TokenWish): string {
  const line = [
    'sensapp generate-token',
    wish.subject.trim() === '' ? 'NAME' : shellWord(wish.subject),
    `--scope ${wish.scope.length > 0 ? wish.scope.join(',') : 'SCOPE'}`,
    `--duration ${wish.durationSeconds}`,
  ].join(' ');
  if (wish.sensors.length === 0) return line;
  return [line, ...wish.sensors.map((name) => `--sensor ${shellWord(name)}`)].join(' \\\n  ');
}
