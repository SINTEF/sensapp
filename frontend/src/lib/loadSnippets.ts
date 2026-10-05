import { SCRIPT_METADATA } from './snippets';

/** What the snippets need to know: where SensApp is, and whether it asks for a token. */
export interface LoadInput {
  /** The origin of the SensApp server */
  baseUrl: string;
  /** The server asked for a token: the snippets read it from `SENSAPP_TOKEN`, they never contain one */
  authenticated: boolean;
}

export type LoadLanguage = 'python' | 'bash' | 'toml' | 'yaml';

/** A piece of the page: a title, a sentence, and the code that goes with it. */
export interface LoadSection {
  title: string;
  description: string;
  language: LoadLanguage;
  code: string;
}

export const LOAD_WAYS = [
  { id: 'python', label: 'Python SDK' },
  { id: 'telegraf', label: 'Telegraf' },
  { id: 'prometheus', label: 'Prometheus' },
  { id: 'curl', label: 'curl' },
] as const;
export type LoadWay = (typeof LOAD_WAYS)[number]['id'];

/** What makes a token that can write. The read one of the Code dialog cannot. */
export const WRITE_TOKEN_COMMAND = 'sensapp generate-token me --scope write';

/** A Python string literal. A JSON string is one, with its escapes. */
const py = (value: string) => JSON.stringify(value);

/** A shell word that is one word whatever it contains. */
const sh = (value: string) => `'${value.replace(/'/g, `'\\''`)}'`;

/** The client of the SDK, as `async with` opens it, indented by `indent`. */
function pythonClient(input: LoadInput, indent: string): string[] {
  return input.authenticated
    ? [`${indent}async with SensAppClient(`, `${indent}    ${py(input.baseUrl)},`, `${indent}    token=os.environ["SENSAPP_TOKEN"],`, `${indent}) as client:`]
    : [`${indent}async with SensAppClient(${py(input.baseUrl)}) as client:`];
}

/** The head of a script: the block `uv run` reads, then the imports. */
function pythonHead(input: LoadInput, file: string, imports: string[]): string[] {
  return [
    ...SCRIPT_METADATA,
    '#',
    `# Run it with \`uv run ${file}\``,
    ...(input.authenticated ? ['#', '# The token comes from the environment, make one with:', `#   export SENSAPP_TOKEN=$(${WRITE_TOKEN_COMMAND})`] : []),
    '',
    ...imports,
  ];
}

export function pythonSections(input: LoadInput): LoadSection[] {
  const os = input.authenticated ? ['import os'] : [];

  const frame = [
    ...pythonHead(input, 'load.py', ['import asyncio', 'from datetime import UTC, datetime, timedelta', ...os, '', 'import polars as pl', 'from sensapp import SensAppClient']),
    '',
    '# One row per sample: "timestamp" (with a time zone) and "value".',
    '# Or read yours: pl.read_csv("data.csv", try_parse_dates=True), pl.read_parquet(...), pl.from_pandas(df).',
    '# A timestamp with no time zone needs .dt.replace_time_zone("UTC").',
    'start = datetime.now(UTC) - timedelta(days=30)',
    'frame = pl.DataFrame(',
    '    {',
    '        "timestamp": [start + timedelta(minutes=i) for i in range(43_200)],',
    '        "value": [20 + (i % 1440) / 100 for i in range(43_200)],',
    '    }',
    ')',
    '',
    '',
    'async def main() -> None:',
    ...pythonClient(input, '    '),
    '        # Slices keep each request small',
    '        for part in frame.iter_slices(100_000):',
    '            await client.publish("temperature", part)',
    '',
    '',
    'asyncio.run(main())',
  ];

  const one = [
    ...pythonHead(input, 'sample.py', ['import asyncio', ...os, '', 'from sensapp import SensAppClient']),
    '',
    '',
    'async def main() -> None:',
    ...pythonClient(input, '    '),
    '        # The timestamp is the time of the call',
    '        await client.publish("temperature", 21.5)',
    '',
    '',
    'asyncio.run(main())',
  ];

  const few = [
    ...pythonHead(input, 'stream.py', ['import asyncio', 'import random', 'from datetime import UTC, datetime', ...os, '', 'from sensapp import SamplePoint, SensAppClient']),
    '',
    '',
    'def read_temperature() -> float:',
    '    return 20 + random.random()  # your sensor here',
    '',
    '',
    'async def main() -> None:',
    ...pythonClient(input, '    '),
    '        batch: list[SamplePoint] = []',
    '        while True:',
    '            # Each sample keeps the time it was read',
    '            batch.append(SamplePoint(datetime.now(UTC), read_temperature()))',
    '            if len(batch) >= 10:',
    '                await client.publish("temperature", batch)',
    '                batch.clear()',
    '            await asyncio.sleep(1)',
    '',
    '',
    'asyncio.run(main())',
  ];

  return [
    {
      title: 'DataFrame',
      description: 'Send a table of timestamps and values. Publishing the same name again appends to the same series.',
      language: 'python',
      code: frame.join('\n') + '\n',
    },
    {
      title: 'One sample at a time',
      description: 'The sample is stamped with the time of the call.',
      language: 'python',
      code: one.join('\n') + '\n',
    },
    {
      title: 'Batches',
      description: 'Keep the samples and send ten at a time: fewer requests than one per sample.',
      language: 'python',
      code: few.join('\n') + '\n',
    },
  ];
}

export function telegrafSections(input: LoadInput): LoadSection[] {
  const config = [
    '[agent]',
    '  interval = "10s"',
    '  flush_interval = "10s"',
    '',
    '# SensApp speaks the InfluxDB v2 write API',
    '[[outputs.influxdb_v2]]',
    `  urls = [${py(input.baseUrl)}]`,
    '  organization = "sensapp"',
    '  bucket = "telegraf"',
    '  content_encoding = "gzip"',
    '  influx_uint_support = true',
    ...(input.authenticated
      ? [
          '  # Make a token with:  ' + WRITE_TOKEN_COMMAND,
          '  # and start Telegraf with it in SENSAPP_TOKEN.',
          '  token = "${SENSAPP_TOKEN}"',
        ]
      : []),
    '',
    '# Something to measure',
    '[[inputs.cpu]]',
    '  percpu = true',
    '  totalcpu = true',
    '',
    '[[inputs.mem]]',
    '',
    '[[inputs.disk]]',
    '  ignore_fs = ["tmpfs", "devtmpfs", "devfs", "overlay", "squashfs"]',
  ];
  return [
    {
      title: 'telegraf.conf',
      description:
        'Telegraf writes through the InfluxDB v2 API. The measurement and the field make the series name (`cpu usage_idle`), the tags become labels, and `organization` and `bucket` are kept as the labels `influxdb_org` and `influxdb_bucket`.',
      language: 'toml',
      code: config.join('\n') + '\n',
    },
    {
      title: 'Run it',
      description: 'One collection to test, then the daemon.',
      language: 'bash',
      code: [
        ...(input.authenticated ? [`export SENSAPP_TOKEN=$(${WRITE_TOKEN_COMMAND})`] : []),
        'telegraf --config telegraf.conf --once',
        'telegraf --config telegraf.conf',
      ].join('\n') + '\n',
    },
  ];
}

const PROMETHEUS_TOKEN_FILE = '/etc/prometheus/sensapp.token';

/** Prometheus reads its token from a file, for writing and for reading. */
const PROMETHEUS_TOKEN_COMMAND = `sensapp generate-token prometheus --scope readwrite --duration 31536000 > ${PROMETHEUS_TOKEN_FILE}`;

export function prometheusSections(input: LoadInput): LoadSection[] {
  const authorization = input.authenticated
    ? ['    authorization:', `      credentials_file: ${PROMETHEUS_TOKEN_FILE}`]
    : [];
  const config = [
    'remote_write:',
    '  - name: sensapp',
    `    url: ${input.baseUrl}/api/v1/prometheus_remote_write`,
    ...authorization,
    '',
    'remote_read:',
    '  - name: sensapp',
    `    url: ${input.baseUrl}/api/v1/prometheus_remote_read`,
    '    read_recent: true',
    ...authorization,
  ];
  return [
    ...(input.authenticated
      ? [
          {
            title: 'A token',
            description: 'This server asks for a token. Prometheus reads it from a file. This one can read and write, and lasts a year (the default is an hour).',
            language: 'bash' as const,
            code: PROMETHEUS_TOKEN_COMMAND + '\n',
          },
        ]
      : []),
    {
      title: 'prometheus.yml',
      description:
        '`remote_write` sends every scraped sample to SensApp, `remote_read` queries it back. Labels are kept, the metric name is the series name. `read_recent` reads recent data from SensApp too, not only what local storage no longer has.',
      language: 'yaml',
      code: config.join('\n') + '\n',
    },
    {
      title: 'Prometheus in a container',
      description: 'In a container `localhost` is the container: use `host.docker.internal`, or the name of the SensApp service.',
      language: 'yaml',
      code: ['remote_write:', '  - url: http://host.docker.internal:3000/api/v1/prometheus_remote_write', ''].join('\n'),
    },
  ];
}

export function curlSections(input: LoadInput): LoadSection[] {
  const header = input.authenticated ? ['-H "Authorization: Bearer $SENSAPP_TOKEN"'] : [];
  const token = input.authenticated ? [`# SENSAPP_TOKEN holds a token, made with:`, `#   ${WRITE_TOKEN_COMMAND}`, ''] : [];
  /** One request: each option on its own line, the address last. */
  const request = (options: string[], url: string) => ['curl -sS --fail-with-body \\', ...[...header, ...options].map((line) => `  ${line} \\`), `  ${url}`];
  const publish = sh(`${input.baseUrl}/publish`);
  const write = sh(`${input.baseUrl}/api/v2/write?org=sensapp&bucket=home&precision=s`);

  const senml = [
    ...token,
    ...request(['--json ' + sh('[{"n":"temperature","u":"Cel","v":21.5},{"n":"humidity","u":"%RH","v":41,"t":1767225600}]')], publish),
  ];
  const csv = [
    ...token,
    "printf 'datetime,sensor_name,value,unit\\n2026-01-01T12:00:00Z,temperature,21.5,Cel\\n2026-01-01T12:01:00Z,temperature,21.7,Cel\\n' > measurements.csv",
    '',
    ...request(['-H ' + sh('content-type: text/csv'), '--data-binary @measurements.csv'], publish),
  ];
  const line = [
    ...token,
    ...request(['--data-binary ' + sh('weather,location=oslo temperature=21.5,humidity=41i 1767225600')], write),
  ];
  return [
    { title: 'SenML JSON', description: 'RFC 8428. `n` is the name, `u` the unit, `v` the value, `t` a Unix time in seconds (now when missing).', language: 'bash', code: senml.join('\n') + '\n' },
    { title: 'CSV', description: 'The columns are found by name: a datetime, a `sensor_name`, a `value`, and optionally a `unit`.', language: 'bash', code: csv.join('\n') + '\n' },
    { title: 'InfluxDB line protocol', description: '`measurement,tag=value field=value timestamp`. The tags become labels. `org` and `bucket` are required, `precision=s` makes the timestamp seconds.', language: 'bash', code: line.join('\n') + '\n' },
  ];
}

export function sectionsFor(way: LoadWay, input: LoadInput): LoadSection[] {
  switch (way) {
    case 'python':
      return pythonSections(input);
    case 'telegraf':
      return telegrafSections(input);
    case 'prometheus':
      return prometheusSections(input);
    case 'curl':
      return curlSections(input);
  }
}
