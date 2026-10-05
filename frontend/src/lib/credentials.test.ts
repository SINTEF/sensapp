import { describe, expect, it } from 'vitest';
import { ADMIN_TOKEN_COMMAND, addSensor, shellWord, tokenCommand, tokenScopes } from './credentials';

describe('addSensor', () => {
  it('adds a name and keeps the order', () => {
    expect(addSensor(addSensor([], 'temperature'), 'humidity')).toEqual(['temperature', 'humidity']);
  });

  it('keeps a name exactly as typed, commas and spaces included', () => {
    expect(addSensor([], 'cpu,usage')).toEqual(['cpu,usage']);
    expect(addSensor([], ' padded ')).toEqual([' padded ']);
    expect(addSensor(['cpu usage_idle'], 'cpu usage_idle')).toEqual(['cpu usage_idle']);
  });

  it('does not add a blank name nor one that is there already', () => {
    const sensors = ['temperature'];
    expect(addSensor(sensors, '')).toBe(sensors);
    expect(addSensor(sensors, '   ')).toBe(sensors);
    expect(addSensor(sensors, 'temperature')).toBe(sensors);
  });
});

describe('tokenScopes', () => {
  it('lists what the token claims', () => {
    expect(tokenScopes({ scope: 'read write delete' })).toEqual(['read', 'write', 'delete']);
    expect(tokenScopes({ scope: 'admin' })).toEqual(['admin']);
  });

  it('knows the default of a token without the claim, and of one that cannot be read', () => {
    expect(tokenScopes({})).toEqual(['read', 'write']);
    expect(tokenScopes(null)).toEqual(['read', 'write']);
  });
});

describe('shellWord', () => {
  it('leaves a plain word bare and quotes the rest', () => {
    expect(shellWord('edge-device_7')).toBe('edge-device_7');
    expect(shellWord('cpu usage')).toBe("'cpu usage'");
    expect(shellWord('cpu,usage')).toBe("'cpu,usage'");
    expect(shellWord("it's")).toBe(`'it'\\''s'`);
    // Nothing a name holds can add a command
    expect(shellWord('x; rm -rf ~')).toBe("'x; rm -rf ~'");
    expect(shellWord('$(whoami)')).toBe("'$(whoami)'");
  });
});

describe('ADMIN_TOKEN_COMMAND', () => {
  it('makes a token that can read as well, so that the explorer does not ask for another', () => {
    expect(ADMIN_TOKEN_COMMAND).toBe('sensapp generate-token me --scope read,admin');
  });
});

describe('tokenCommand', () => {
  const wish = { subject: 'edge-7', scope: ['read', 'write'], sensors: [], durationSeconds: 86400 };

  it('is one line without sensors', () => {
    expect(tokenCommand(wish)).toBe('sensapp generate-token edge-7 --scope read,write --duration 86400');
  });

  it('gives each sensor a --sensor of its own, commas kept in the name', () => {
    expect(tokenCommand({ ...wish, sensors: ['temperature', 'cpu,usage', 'mem used'] })).toBe(
      "sensapp generate-token edge-7 --scope read,write --duration 86400 \\\n  --sensor temperature \\\n  --sensor 'cpu,usage' \\\n  --sensor 'mem used'",
    );
  });

  it('quotes the name of the token', () => {
    expect(tokenCommand({ ...wish, subject: "bob's phone" })).toContain(`'bob'\\''s phone'`);
  });

  it('says what is still to choose', () => {
    expect(tokenCommand({ ...wish, subject: '  ', scope: [] })).toBe('sensapp generate-token NAME --scope SCOPE --duration 86400');
  });

  it('can make an admin token, which the endpoint cannot', () => {
    expect(tokenCommand({ ...wish, scope: ['admin'], durationSeconds: 3600 })).toBe('sensapp generate-token edge-7 --scope admin --duration 3600');
  });
});
