import { isNumericType } from './chartStep';
import type { Aggregation } from './chartStep';
import { sharedLabels, withoutShared } from './seriesLabels';
import type { TimeRange } from './timeRange';

/** What a snippet says of the explorer: what is selected, and how it is read. */
export interface SnippetInput {
  /** The origin of the SensApp server */
  baseUrl: string;
  /** The server asked for a token: the snippet reads it from `SENSAPP_TOKEN`, it never contains one */
  authenticated: boolean;
  metric: string | null;
  /** The selector of the series list (`{host="a"}`). Set, the code asks the server for the series that match it. */
  selector?: string;
  series: Array<{ uuid: string; name: string; labels: Record<string, string>; type: string }>;
  timeRange: TimeRange;
  /** A preset such as `24h`: the window is written relative to now, as it is on screen */
  relativeRange: string | null;
  /** The step of the chart, `undefined` for the raw samples */
  step: string | undefined;
  aggregation: Aggregation;
}

export const SNIPPET_LANGUAGES = ['python', 'curl'] as const;
export type SnippetLanguage = (typeof SNIPPET_LANGUAGES)[number];

const GIT_URL = 'https://github.com/SINTEF/sensapp.git';
export const INSTALL_COMMENT = `uv pip install 'git+${GIT_URL}@main#subdirectory=python/sensapp'`;

/** The block that `uv run` reads (PEP 723): the script says what it needs, and where the SDK comes from. */
export const SCRIPT_METADATA = [
  '# /// script',
  '# requires-python = ">=3.14"',
  '# dependencies = ["sensapp"]',
  '#',
  '# [tool.uv.sources.sensapp]',
  `# git = "${GIT_URL}"`,
  '# subdirectory = "python/sensapp"',
  '# branch = "main"',
  '# ///',
];
export const TOKEN_COMMENT = 'sensapp generate-token me --scope read';

/** A line of text for a comment. A newline in a label must not turn the rest into code. */
function oneLine(text: string): string {
  return text.replace(/[\p{Cc}\p{Zl}\p{Zp}]+/gu, ' ');
}

function pairs(labels: Record<string, string>): string {
  return Object.entries(labels)
    .map(([key, value]) => `${key}="${value}"`)
    .join(', ');
}

/**
 * What the series are called in the comments: the name and the labels that tell a series from the others,
 * as in the list of the explorer. What every series has is said once, in `shared`.
 */
function names(series: SnippetInput['series']): { label: (s: SnippetInput['series'][number]) => string; shared: string | null } {
  const common = sharedLabels(series.map((s) => s.labels));
  return {
    label: (s) => {
      const own = pairs(withoutShared(s.labels, common));
      return oneLine(own ? `${s.name}{${own}}` : s.name);
    },
    shared: Object.keys(common).length > 0 ? oneLine(`On every series: ${pairs(common)}`) : null,
  };
}

/** A Python string literal. A JSON string is one, with its escapes. */
function py(value: string): string {
  return JSON.stringify(value);
}

/** The same, in single quotes when that says it with fewer backslashes: `'{host="a"}'`. */
function pyQuoted(value: string): string {
  return value.includes('"') && !/['\\\p{Cc}\p{Zl}\p{Zp}]/u.test(value) ? `'${value}'` : py(value);
}

/** What goes inside double quotes in the shell, where `$uuid` is read: the rest has to be escaped. */
function escapeDouble(value: string): string {
  return value.replace(/[\\"$`]/g, '\\$&');
}

/** A shell word that is one word whatever it contains. */
function sh(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

/** The date the way a person writes it: `2026-10-05T09:00:00Z`, the milliseconds only when there are some. */
function clean(iso: string): string {
  return iso.replace(/\.000Z$/, 'Z');
}

/** `24h` as `hours=24`, the argument of a Python `timedelta`. */
function timedelta(label: string): string | undefined {
  const match = /^(\d+)([mhd])$/.exec(label);
  if (!match) return undefined;
  const unit = { m: 'minutes', h: 'hours', d: 'days' }[match[2] as 'm' | 'h' | 'd'];
  return `${unit}=${match[1]}`;
}

/** The series that the step can average, and the ones it cannot (booleans, strings): those are read as they are. */
function groups(input: SnippetInput): Array<{ series: SnippetInput['series']; step?: string }> {
  if (!input.step) return [{ series: input.series }];
  const aggregated = input.series.filter((s) => isNumericType(s.type));
  const raw = input.series.filter((s) => !isNumericType(s.type));
  return [
    ...(aggregated.length > 0 ? [{ series: aggregated, step: input.step }] : []),
    ...(raw.length > 0 ? [{ series: raw }] : []),
  ];
}

/** The selector to use, if the user typed one. */
function selectorOf(input: SnippetInput): string | undefined {
  return input.selector?.trim() || undefined;
}

export function pythonSnippet(input: SnippetInput): string {
  const delta = input.relativeRange ? timedelta(input.relativeRange) : undefined;
  const selector = selectorOf(input);
  const reads = input.series.length > 0 || selector !== undefined;
  const lines: string[] = [...SCRIPT_METADATA];

  lines.push('#', '# Run it with `uv run script.py`, or install the SDK first:', `#   ${INSTALL_COMMENT}`);
  if (input.authenticated) lines.push('#', '# The token comes from the environment, make one with:', `#   export SENSAPP_TOKEN=$(${TOKEN_COMMENT})`);
  lines.push('', 'import asyncio');
  if (input.authenticated) lines.push('import os');
  if (delta && reads) lines.push('from datetime import UTC, datetime, timedelta');
  lines.push('', 'from sensapp import SensAppClient');

  const client = input.authenticated
    ? [`    async with SensAppClient(`, `        ${py(input.baseUrl)},`, `        token=os.environ["SENSAPP_TOKEN"],`, `    ) as client:`]
    : [`    async with SensAppClient(${py(input.baseUrl)}) as client:`];

  if (!reads) {
    lines.push('', '', 'async def main() -> None:', ...client);
    if (input.metric) {
      lines.push(
        `        catalog = await client.list_series(metric=${py(input.metric)})`,
        '        for info in catalog.series:',
        '            print(info.uuid, info.name, info.labels)',
      );
    } else {
      lines.push(
        '        catalog = await client.list_metrics()',
        '        for metric in catalog.metrics:',
        '            print(metric.name, metric.sensor_type, metric.series_count)',
      );
    }
    lines.push('', '', 'asyncio.run(main())');
    return lines.join('\n') + '\n';
  }

  // What the window is: now minus a duration, or the two dates
  const window: string[] = delta
    ? ['    end = datetime.now(UTC)', `    start = end - timedelta(${delta})`]
    : [`    start = ${py(clean(input.timeRange.start))}`, `    end = ${py(clean(input.timeRange.end))}`];
  const [start, end] = delta ? ['start.isoformat(timespec="seconds")', 'end.isoformat(timespec="seconds")'] : ['start', 'end'];
  const read = (indent: string, id: string, step?: string[]) => [
    `${indent}series = await client.get_series(`,
    `${indent}    ${id},`,
    `${indent}    start=${start},`,
    `${indent}    end=${end},`,
    ...(step ?? []).map((line) => `${indent}    ${line}`),
    `${indent})`,
    `${indent}print(series.name, series.labels)`,
    `${indent}print(series.frame)  # a Polars DataFrame: timestamp, value`,
  ];

  if (selector !== undefined) {
    // The series are known when the code runs, not now: a step is for the numbers among them
    lines.push('', '', 'async def main() -> None:', ...window, ...client);
    lines.push(
      '        catalog = await client.list_series(',
      ...(input.metric ? [`            metric=${py(input.metric)},`] : []),
      `            selector=${pyQuoted(selector)},`,
      '        )',
      '        for info in catalog.series:',
    );
    if (input.step) {
      lines.push(
        '            # A step averages numbers: the other types are read as they are',
        '            numeric = info.sensor_type.lower() in ("integer", "float", "numeric")',
      );
    }
    lines.push(
      ...read(
        '            ',
        'info.uuid',
        input.step ? [`step=${py(input.step)} if numeric else None,`, `aggregation=${py(input.aggregation)} if numeric else None,`] : undefined,
      ),
      '',
      '',
      'asyncio.run(main())',
    );
    return lines.join('\n') + '\n';
  }

  const parts = groups(input);
  const mixed = parts.length > 1;
  const variables = parts.map((part) => (mixed && !part.step ? 'RAW_SERIES' : 'SERIES'));
  const { label, shared } = names(input.series);

  parts.forEach((part, index) => {
    lines.push('');
    if (index === 0 && shared) lines.push(`# ${shared}`);
    lines.push(`${variables[index]} = [`);
    for (const s of part.series) lines.push(`    ${py(s.uuid)},  # ${label(s)}`);
    lines.push(']');
  });

  lines.push('', '', 'async def main() -> None:', ...window, ...client);
  parts.forEach((part, index) => {
    lines.push(
      `        for uuid in ${variables[index]}:`,
      ...read('            ', 'uuid', part.step ? [`step=${py(part.step)},`, `aggregation=${py(input.aggregation)},`] : undefined),
    );
  });
  lines.push('', '', 'asyncio.run(main())');
  return lines.join('\n') + '\n';
}

export function curlSnippet(input: SnippetInput): string {
  const lines: string[] = [];
  const selector = selectorOf(input);
  const { label, shared } = names(input.series);
  if (input.authenticated) {
    lines.push('# SENSAPP_TOKEN holds a token, made with:', `#   ${TOKEN_COMMENT}`, '');
  }
  if (selector === undefined && shared) lines.push(`# ${shared}`, '');
  const auth = input.authenticated ? ['-H "Authorization: Bearer $SENSAPP_TOKEN"'] : [];

  /** The lines of a request: each option on its own line, the last one the address. */
  const request = (indent: string, options: string[], url: string) => [
    `${indent}curl -sS --fail-with-body \\`,
    ...[...auth, ...options].map((line) => `${indent}  ${line} \\`),
    `${indent}  ${url}`,
  ];
  const readParams = (step: string | undefined) => {
    const params = new URLSearchParams({ format: 'csv', start: clean(input.timeRange.start), end: clean(input.timeRange.end) });
    if (step) {
      params.set('step', step);
      params.set('aggregation', input.aggregation);
    }
    // The colons of the dates are fine in a query: leave them readable
    return params.toString().replace(/%3A/g, ':');
  };

  const blocks: string[][] = [];
  if (selector !== undefined) {
    // The series that match, one after the other. A step averages numbers: the others are left out of that read.
    const jq = input.step
      ? `.["dcat:dataset"][] | select(.["sensor:type"] | ascii_downcase | IN("integer", "float", "numeric")) | .["dct:identifier"]`
      : '.["dcat:dataset"][]["dct:identifier"]';
    const list = request(
      '',
      ['-G', ...(input.metric ? [`--data-urlencode ${sh(`metric=${input.metric}`)}`] : []), `--data-urlencode ${sh(`selector=${selector}`)}`],
      `${sh(`${input.baseUrl}/series`)} |`,
    );
    const each = request('  ', [], `"${escapeDouble(input.baseUrl)}/series/$uuid?${readParams(input.step)}"`);
    blocks.push([
      `# The series that match ${oneLine(selector)}${input.step ? ' (the numeric ones: a step averages numbers)' : ''}`,
      ...list,
      `  jq -r ${sh(jq)} |`,
      '  while read -r uuid; do',
      ...each.map((line) => `  ${line}`),
      '  done',
    ]);
  } else if (input.series.length === 0) {
    blocks.push(
      request(
        '',
        [],
        sh(input.metric ? `${input.baseUrl}/series?${new URLSearchParams({ metric: input.metric })}` : `${input.baseUrl}/metrics`),
      ),
    );
  } else {
    for (const part of groups(input)) {
      for (const s of part.series) {
        blocks.push([`# ${label(s)}`, ...request('', [], sh(`${input.baseUrl}/series/${encodeURIComponent(s.uuid)}?${readParams(part.step)}`))]);
      }
    }
  }
  return lines.concat(blocks.map((block) => block.join('\n')).join('\n\n')).join('\n') + '\n';
}

export function snippetFor(language: SnippetLanguage, input: SnippetInput): string {
  return language === 'python' ? pythonSnippet(input) : curlSnippet(input);
}
