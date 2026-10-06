export type Labels = Record<string, string>;

/** `node-2` before `node-10`, and `A` next to `a`. */
export function naturalCompare(a: string, b: string): number {
  return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' });
}

/**
 * The labels that have the same value on every series, which is nothing for a single series (it would
 * say everything it has). Said once above a list, they would only repeat in a column.
 */
export function sharedLabels(all: Labels[]): Labels {
  if (all.length < 2) return {};
  const [first, ...others] = all;
  return Object.fromEntries(
    Object.entries(first).filter(([key, value]) => others.every((labels) => labels[key] === value)),
  );
}

/** `cpu{host="a", core="1"}`, or the name alone when there is no label. */
export function seriesLabel(name: string, labels: Labels): string {
  const text = Object.entries(labels)
    .map(([k, v]) => `${k}="${v}"`)
    .join(', ');
  return text ? `${name}{${text}}` : name;
}

/** The labels of a series that tell it from the others of a selection. */
export function withoutShared(labels: Labels, shared: Labels): Labels {
  return Object.fromEntries(Object.entries(labels).filter(([key]) => !(key in shared)));
}

/**
 * Series ordered by the columns in turn: the one that was clicked first, then the others in their
 * order, a missing label coming last. Numbers inside the values are read as numbers.
 */
export function sortByColumns<T>(
  items: T[],
  labelsOf: (item: T) => Partial<Labels>,
  columns: string[],
  first: string | undefined,
  descending: boolean,
): T[] {
  const order = first ? [first, ...columns.filter((c) => c !== first)] : columns;
  return [...items].sort((a, b) => {
    for (const [index, column] of order.entries()) {
      const left = labelsOf(a)[column];
      const right = labelsOf(b)[column];
      if (left === right) continue;
      if (left === undefined) return 1;
      if (right === undefined) return -1;
      const result = naturalCompare(left, right);
      if (result !== 0) return index === 0 && descending ? -result : result;
    }
    return 0;
  });
}

const quote = (value: string) => `"${value.replace(/[\\"]/g, '\\$&')}"`;

/**
 * A selector made of what the series have, to show how to write one: the first label equals its value,
 * the second starts like its value (`{room="lab", floor=~"1.*"}`).
 */
export function exampleSelector(labels: Labels | undefined, columns: string[]): string {
  const known = columns.filter((column) => labels?.[column] !== undefined).slice(0, 2);
  if (!labels || known.length === 0) return '{label="value", other=~"va.*"}';
  const [one, two] = known;
  const parts = [`${one}=${quote(labels[one])}`];
  if (two) {
    const start = labels[two].slice(0, 1).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    parts.push(`${two}=~${quote(`${start}.*`)}`);
  }
  return `{${parts.join(', ')}}`;
}
