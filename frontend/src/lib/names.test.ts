import { describe, expect, it } from 'vitest';
import { randomName } from './names';

describe('randomName', () => {
  it('is two adjectives, a scientist and a number, joined by dashes', () => {
    for (let i = 0; i < 200; i++) expect(randomName()).toMatch(/^[a-z]+-[a-z]+-[a-z]+-\d{1,2}$/);
  });

  it('is made of what the random numbers pick', () => {
    expect(randomName(() => 0)).toBe('admiring-adoring-agnesi-0');
  });

  it('does not repeat an adjective', () => {
    for (let i = 0; i < 200; i++) {
      const [first, second] = randomName().split('-');
      expect(first).not.toBe(second);
    }
  });
});
