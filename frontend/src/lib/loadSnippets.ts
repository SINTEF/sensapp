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
    '# A table of samples: a "timestamp" column with a time zone, and a "value" column.',
    '# Read yours instead: pl.read_csv("measurements.csv", try_parse_dates=True), pl.read_parquet(...)',
    '# or pl.from_pandas(df). A timestamp with no time zone is UTC after .dt.replace_time_zone("UTC").',
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
    '        # A history goes by slices: a request is not meant to carry everything',
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
    '        # The sample is stamped with the time it is sent',
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
      title: 'A whole DataFrame',
      description: 'A history, or a file you have: send the table, SensApp keeps its timestamps. Publishing the same name again adds to the same series.',
      language: 'python',
      code: frame.join('\n') + '\n',
    },
    {
      title: 'One sample at a time',
      description: 'The simplest thing that works, for a sensor that reports now and then.',
      language: 'python',
      code: one.join('\n') + '\n',
    },
    {
      title: 'A few samples at a time',
      description: 'A sensor that reports often: keep the samples for a moment and send them together, which costs the server less than a request each.',
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
          '  # Telegraf sends its token as "Authorization: Token ...", SensApp wants "Bearer".',
          '  # Make a token with:  ' + WRITE_TOKEN_COMMAND,
          '  # and start Telegraf with it in SENSAPP_TOKEN.',
          '  http_headers = {"Authorization" = "Bearer ${SENSAPP_TOKEN}"}',
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
        'Telegraf writes as it would to InfluxDB. A measurement and a field make the name of a series (`cpu` and `usage_idle` give `cpu usage_idle`), the tags become its labels, and `organization` and `bucket` are kept as the labels `influxdb_org` and `influxdb_bucket`.',
      language: 'toml',
      code: config.join('\n') + '\n',
    },
    {
      title: 'Run it',
      description: 'Check it with one collection, then leave it running.',
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
            description: 'This server asks for one. Prometheus reads it from a file; this one writes and reads, and is good for a year (the default is an hour).',
            language: 'bash' as const,
            code: PROMETHEUS_TOKEN_COMMAND + '\n',
          },
        ]
      : []),
    {
      title: 'prometheus.yml',
      description:
        'Every sample Prometheus scrapes is also sent to SensApp. The labels are kept as they are, and the metric name is the name of the series. Add the remote_read to query SensApp from Prometheus and Grafana too; `read_recent` makes Prometheus ask it for recent data as well, not only for what its own storage has dropped.',
      language: 'yaml',
      code: config.join('\n') + '\n',
    },
    {
      title: 'Prometheus in a container',
      description: 'A container does not reach SensApp at `localhost`: use `host.docker.internal` (Docker Desktop) or the name of the SensApp service in the same network.',
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
    '# "n" is the name, "u" the unit and "v" the value. "t" is a Unix time in seconds: left out, it is now.',
    ...request(['--json ' + sh('[{"n":"temperature","u":"Cel","v":21.5},{"n":"humidity","u":"%RH","v":41,"t":1767225600}]')], publish),
  ];
  const csv = [
    ...token,
    '# A file with a datetime column, a sensor_name column and a value column (and a unit one, if you like)',
    "printf 'datetime,sensor_name,value,unit\\n2026-01-01T12:00:00Z,temperature,21.5,Cel\\n2026-01-01T12:01:00Z,temperature,21.7,Cel\\n' > measurements.csv",
    '',
    ...request(['-H ' + sh('content-type: text/csv'), '--data-binary @measurements.csv'], publish),
  ];
  const line = [
    ...token,
    '# measurement,tag=value field=value timestamp: the tags become labels. precision=s says the timestamp is in seconds.',
    ...request(['--data-binary ' + sh('weather,location=oslo temperature=21.5,humidity=41i 1767225600')], write),
  ];
  return [
    { title: 'SenML JSON', description: 'The standard format for sensor data (RFC 8428), and the one that needs the least.', language: 'bash', code: senml.join('\n') + '\n' },
    { title: 'CSV', description: 'A file out of a spreadsheet or a logger. SensApp finds the columns by their names.', language: 'bash', code: csv.join('\n') + '\n' },
    { title: 'InfluxDB line protocol', description: 'What Telegraf and many devices speak. `org` and `bucket` are required, as for InfluxDB.', language: 'bash', code: line.join('\n') + '\n' },
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
