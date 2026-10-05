import { describe, expect, it } from 'vitest';
import { exampleSelector, sharedLabels, sortByColumns, withoutShared } from './seriesLabels';

describe('sharedLabels', () => {
  it('keeps what every series has the same, nothing for one series', () => {
    const list = [
      { org: 'a', room: 'lab' },
      { org: 'a', room: 'office' },
    ];
    expect(sharedLabels(list)).toEqual({ org: 'a' });
    expect(sharedLabels([list[0]])).toEqual({});
    expect(sharedLabels([])).toEqual({});
  });

  it('is not shared when one series lacks the label', () => {
    expect(sharedLabels([{ org: 'a' }, { org: 'a' }, {}])).toEqual({});
  });

  it('takes the shared labels out of one series', () => {
    expect(withoutShared({ org: 'a', room: 'lab' }, { org: 'a' })).toEqual({ room: 'lab' });
  });
});

describe('sortByColumns', () => {
  const rows = [
    { host: 'node-10', dc: 'b' },
    { host: 'node-2', dc: 'b' },
    { host: 'node-1', dc: 'a' },
    { dc: 'a' },
  ];
  const sort = (first?: string, descending = false) =>
    sortByColumns(rows, (r) => r, ['host', 'dc'], first, descending).map((r) => `${r.host ?? '-'}/${r.dc}`);

  it('orders by the first column, numbers as numbers, what lacks it last', () => {
    expect(sort()).toEqual(['node-1/a', 'node-2/b', 'node-10/b', '-/a']);
  });

  it('orders by the column that was chosen, then by the others', () => {
    expect(sort('dc')).toEqual(['node-1/a', '-/a', 'node-2/b', 'node-10/b']);
  });

  it('can be reversed, what lacks the label staying last', () => {
    expect(sort('host', true)).toEqual(['node-10/b', 'node-2/b', 'node-1/a', '-/a']);
  });

  it('leaves its input alone', () => {
    sort();
    expect(rows[0].host).toBe('node-10');
  });
});

describe('exampleSelector', () => {
  it('takes two labels of the series, the second as a start', () => {
    expect(exampleSelector({ room: 'lab', floor: '1', other: 'x' }, ['room', 'floor', 'other'])).toBe(
      '{room="lab", floor=~"1.*"}',
    );
  });

  it('works with one label, and with a series that has none', () => {
    expect(exampleSelector({ room: 'lab' }, ['room'])).toBe('{room="lab"}');
    expect(exampleSelector({}, [])).toBe('{label="value", other=~"va.*"}');
    expect(exampleSelector(undefined, ['room'])).toBe('{label="value", other=~"va.*"}');
  });

  it('skips a label the series does not have, and escapes what has to be', () => {
    expect(exampleSelector({ b: 'say "hi"', c: '.x' }, ['a', 'b', 'c'])).toBe(String.raw`{b="say \"hi\"", c=~"\\..*"}`);
  });
});
