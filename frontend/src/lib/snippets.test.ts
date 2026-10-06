import { describe, expect, it } from 'vitest';
import { curlSnippet, pythonSnippet } from './snippets';
import type { SnippetInput } from './snippets';

const UUID_A = '11111111-1111-4111-8111-111111111111';
const UUID_B = '22222222-2222-4222-8222-222222222222';

const base: SnippetInput = {
  baseUrl: 'http://localhost:3000',
  authenticated: false,
  metric: 'cpu',
  series: [{ uuid: UUID_A, name: 'cpu', labels: { host: 'a' }, type: 'float' }],
  timeRange: { start: '2026-10-05T09:00:00.000Z', end: '2026-10-05T10:00:00.000Z' },
  relativeRange: null,
  step: undefined,
  aggregation: 'avg',
};

describe('pythonSnippet', () => {
  it('reads a window of dates as they are, with the name of the series in a comment', () => {
    const code = pythonSnippet(base);
    expect(code).toContain('from sensapp import SensAppClient');
    expect(code).toContain(`    "${UUID_A}",  # cpu{host="a"}`);
    expect(code).toContain('start = "2026-10-05T09:00:00Z"');
    expect(code).toContain('end = "2026-10-05T10:00:00Z"');
    expect(code).toContain('async with SensAppClient("http://localhost:3000") as client:');
    expect(code).toContain('                start=start,');
    expect(code).not.toContain('step=');
    expect(code).not.toContain('datetime');
    expect(code).not.toContain('SENSAPP_TOKEN');
  });

  it('writes a preset window relative to now', () => {
    const code = pythonSnippet({ ...base, relativeRange: '24h' });
    expect(code).toContain('from datetime import UTC, datetime, timedelta');
    expect(code).toContain('end = datetime.now(UTC)');
    expect(code).toContain('start = end - timedelta(hours=24)');
    expect(code).toContain('start=start.isoformat(timespec="seconds"),');
    expect(code).not.toContain('2026-10-05');
    expect(pythonSnippet({ ...base, relativeRange: '7d' })).toContain('timedelta(days=7)');
    expect(pythonSnippet({ ...base, relativeRange: '15m' })).toContain('timedelta(minutes=15)');
    // Python has no year
    expect(pythonSnippet({ ...base, relativeRange: '1y' })).toContain('timedelta(days=365)');
  });

  it('asks for the step and the aggregation of the chart', () => {
    const code = pythonSnippet({ ...base, step: '5m', aggregation: 'max' });
    expect(code).toContain('step="5m",');
    expect(code).toContain('aggregation="max",');
  });

  it('reads the token from the environment, and never has one', () => {
    const code = pythonSnippet({ ...base, authenticated: true });
    expect(code).toContain('import os');
    expect(code).toContain('token=os.environ["SENSAPP_TOKEN"],');
    expect(code).toContain('sensapp generate-token');
  });

  it('keeps the series a step cannot average apart, read as they are', () => {
    const code = pythonSnippet({
      ...base,
      step: '1m',
      series: [
        base.series[0],
        { uuid: UUID_B, name: 'door', labels: {}, type: 'boolean' },
      ],
    });
    expect(code).toContain(`SERIES = [\n    "${UUID_A}"`);
    expect(code).toContain(`RAW_SERIES = [\n    "${UUID_B}",  # door\n]`);
    expect(code.match(/step="1m"/g)).toHaveLength(1);
    expect(code).toContain('for uuid in RAW_SERIES:');
  });

  it('lists the series of the metric when none is selected, the metrics when there is no metric', () => {
    const metric = pythonSnippet({ ...base, series: [] });
    expect(metric).toContain('client.list_series(metric="cpu")');
    expect(metric).not.toContain('get_series');
    const none = pythonSnippet({ ...base, series: [], metric: null });
    expect(none).toContain('client.list_metrics()');
  });

  it('is not tricked by a label: quotes are escaped and a newline does not end the comment', () => {
    const code = pythonSnippet({
      ...base,
      baseUrl: 'http://host"; import os #',
      series: [
        { uuid: UUID_A, name: 'cpu', labels: { host: 'a"\nimport os; os.system("x")\r\u2028b' }, type: 'float' },
      ],
    });
    // Every line of the comment stays on its line
    const line = code.split('\n').find((l) => l.includes(UUID_A));
    expect(line).toContain('import os; os.system(');
    expect(line?.startsWith('    "')).toBe(true);
    expect(code.split('\n').filter((l) => l.startsWith('import os'))).toHaveLength(0);
    expect(code).toContain('"http://host\\"; import os #"');
  });
});

describe('the script header', () => {
  it('lets `uv run` install the SDK (PEP 723), and keeps the install command for the others', () => {
    const code = pythonSnippet(base);
    expect(code.startsWith('# /// script\n# requires-python = ">=3.14"\n# dependencies = ["sensapp"]\n')).toBe(true);
    expect(code).toContain(
      '# [tool.uv.sources.sensapp]\n# git = "https://github.com/SINTEF/sensapp.git"\n# subdirectory = "python/sensapp"\n# branch = "main"\n# ///',
    );
    expect(code).toContain('# ///\n');
    expect(code).toContain("#   uv pip install 'git+https://github.com/SINTEF/sensapp.git@main#subdirectory=python/sensapp'");
    // The block closes before the code starts
    expect(code.indexOf('# ///\n', 5)).toBeLessThan(code.indexOf('import asyncio'));
  });
});

describe('a selector', () => {
  const selected: SnippetInput = { ...base, selector: '{host="a"}', metric: 'cpu value' };

  it('asks the server for the series that match, in Python', () => {
    const code = pythonSnippet(selected);
    expect(code).toContain('catalog = await client.list_series(\n            metric="cpu value",\n            selector=\'{host="a"}\',\n        )');
    expect(code).toContain('for info in catalog.series:');
    expect(code).toContain('info.uuid,');
    expect(code).not.toContain(UUID_A);
    expect(code).not.toContain('SERIES');
    // No step: nothing to say about types
    expect(code).not.toContain('numeric');
  });

  it('reads a step only for the numbers among the series, whose type is not known yet', () => {
    const code = pythonSnippet({ ...selected, step: '5m', aggregation: 'max' });
    expect(code).toContain('numeric = info.sensor_type.lower() in ("integer", "float", "numeric")');
    expect(code).toContain('step="5m" if numeric else None,');
    expect(code).toContain('aggregation="max" if numeric else None,');
  });

  it('keeps the window of the explorer', () => {
    expect(pythonSnippet({ ...selected, relativeRange: '24h' })).toContain('start = end - timedelta(hours=24)');
    expect(pythonSnippet(selected)).toContain('start = "2026-10-05T09:00:00Z"');
  });

  it('quotes the selector whatever it holds', () => {
    const both = pythonSnippet({ ...selected, selector: `{host=~"a.*", note="it's"}` });
    expect(both).toContain('selector="{host=~\\"a.*\\", note=\\"it\'s\\"}",');
    const ugly = pythonSnippet({ ...selected, selector: '{a="\n1"}' });
    expect(ugly.split('\n').filter((l) => l.startsWith('1"'))).toHaveLength(0);
  });

  it('lists with curl, then reads each series with jq and a loop', () => {
    const code = curlSnippet({ ...selected, authenticated: true });
    expect(code).toContain("  --data-urlencode 'metric=cpu value' \\\n  --data-urlencode 'selector={host=\"a\"}' \\\n  'http://localhost:3000/series' |");
    expect(code).toContain(`jq -r '.["dcat:dataset"][]["dct:identifier"]' |`);
    expect(code).toContain('while read -r uuid; do');
    // $uuid is the one thing the shell has to read
    expect(code).toContain('"http://localhost:3000/series/$uuid?format=csv&start=2026-10-05T09:00:00Z&end=2026-10-05T10:00:00Z"');
    expect(code.match(/Authorization: Bearer \$SENSAPP_TOKEN/g)).toHaveLength(2);
    expect(code).not.toContain(UUID_A);
  });

  it('leaves the series a step cannot average out of the curl loop, and says so', () => {
    const code = curlSnippet({ ...selected, step: '5m', aggregation: 'max' });
    expect(code).toContain('select(.["sensor:type"] | ascii_downcase | IN("integer", "float", "numeric"))');
    expect(code).toContain('step=5m&aggregation=max"');
    expect(code).toContain('the numeric ones');
  });

  it('is not tricked by the selector, or by the address, in the shell', () => {
    const code = curlSnippet({ ...selected, baseUrl: 'http://h"$(rm -rf ~)`x', selector: `{a="it's"}\nrm -rf ~` });
    expect(code).toContain(`--data-urlencode 'selector={a="it'\\''s"}\nrm -rf ~'`);
    expect(code).toContain('"http://h\\"\\$(rm -rf ~)\\`x/series/$uuid?');
    // The comment stays on its line: a newline in the selector is only ever inside quotes
    expect(code).toContain(`# The series that match {a="it's"} rm -rf ~\n`);
  });

  it('is ignored when it is blank', () => {
    expect(pythonSnippet({ ...selected, selector: '  ' })).toContain(UUID_A);
  });
});

describe('what the series are called', () => {
  const two: SnippetInput = {
    ...base,
    series: [
      { uuid: UUID_A, name: 'cpu', labels: { host: 'a', org: 'o' }, type: 'float' },
      { uuid: UUID_B, name: 'cpu', labels: { host: 'b', org: 'o' }, type: 'float' },
    ],
  };

  it('says what every series has once, and what tells them apart on each', () => {
    const python = pythonSnippet(two);
    expect(python).toContain('# On every series: org="o"\nSERIES = [');
    expect(python).toContain(`"${UUID_A}",  # cpu{host="a"}`);
    const curl = curlSnippet(two);
    expect(curl).toContain('# On every series: org="o"\n');
    expect(curl).toContain('# cpu{host="b"}\ncurl');
  });

  it('says everything of a single series', () => {
    const one = { ...two, series: [two.series[0]] };
    expect(pythonSnippet(one)).toContain('# cpu{host="a", org="o"}');
    expect(pythonSnippet(one)).not.toContain('On every series');
  });
});

describe('curlSnippet', () => {
  it('is one request for each series, with the window and the name in a comment', () => {
    const code = curlSnippet({
      ...base,
      series: [base.series[0], { uuid: UUID_B, name: 'mem', labels: {}, type: 'integer' }],
    });
    expect(code.match(/^curl /gm)).toHaveLength(2);
    expect(code).toContain('# cpu{host="a"}');
    expect(code).toContain(
      `'http://localhost:3000/series/${UUID_A}?format=csv&start=2026-10-05T09:00:00Z&end=2026-10-05T10:00:00Z'`,
    );
    expect(code).toContain(`/series/${UUID_B}?`);
    expect(code).not.toContain('Authorization');
  });

  it('asks for the step and the aggregation, except for what cannot be averaged', () => {
    const code = curlSnippet({
      ...base,
      step: '5m',
      aggregation: 'max',
      series: [base.series[0], { uuid: UUID_B, name: 'door', labels: {}, type: 'boolean' }],
    });
    expect(code).toContain(`/series/${UUID_A}?format=csv&start=2026-10-05T09:00:00Z&end=2026-10-05T10:00:00Z&step=5m&aggregation=max'`);
    expect(code).toContain(`/series/${UUID_B}?format=csv&start=2026-10-05T09:00:00Z&end=2026-10-05T10:00:00Z'`);
  });

  it('sends the token of the environment', () => {
    const code = curlSnippet({ ...base, authenticated: true });
    expect(code).toContain('-H "Authorization: Bearer $SENSAPP_TOKEN" \\');
    expect(code).toContain('# SENSAPP_TOKEN holds a token');
  });

  it('lists the series of the metric, or the metrics', () => {
    expect(curlSnippet({ ...base, series: [] })).toContain("'http://localhost:3000/series?metric=cpu'");
    expect(curlSnippet({ ...base, series: [], metric: null })).toContain("'http://localhost:3000/metrics'");
  });

  it('is not tricked by a quote or a newline in what it quotes', () => {
    const code = curlSnippet({
      ...base,
      baseUrl: "http://host'; rm -rf ~; echo '",
      series: [{ uuid: UUID_A, name: 'cpu', labels: { host: 'a\nrm -rf ~' }, type: 'float' }],
    });
    expect(code).toContain(`'http://host'\\''; rm -rf ~; echo '\\''/series/`);
    expect(code.split('\n').filter((l) => l.startsWith('rm '))).toHaveLength(0);
  });
});
