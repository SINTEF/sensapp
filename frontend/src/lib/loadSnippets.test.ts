import { describe, expect, it } from 'vitest';
import { LOAD_WAYS, sectionsFor } from './loadSnippets';
import type { LoadInput } from './loadSnippets';

const open: LoadInput = { baseUrl: 'http://sensapp.example:3000', authenticated: false };
const secured: LoadInput = { ...open, authenticated: true };

const codeOf = (way: Parameters<typeof sectionsFor>[0], input: LoadInput) => sectionsFor(way, input).map((s) => s.code).join('\n');

describe('load snippets', () => {
  it('say where the server is, in every way', () => {
    for (const way of LOAD_WAYS) {
      expect(codeOf(way.id, open), way.id).toContain('sensapp.example:3000');
    }
  });

  it('never carry a token, and read it from the environment only when the server asks for one', () => {
    for (const way of LOAD_WAYS) {
      expect(codeOf(way.id, open), way.id).not.toContain('SENSAPP_TOKEN');
      const code = codeOf(way.id, secured);
      // Prometheus reads a file, the others the environment
      expect(code, way.id).toContain(way.id === 'prometheus' ? 'generate-token prometheus --scope readwrite' : 'SENSAPP_TOKEN');
      if (way.id !== 'prometheus') expect(code, way.id).toContain('generate-token me --scope write');
      expect(code, way.id).not.toMatch(/eyJ/);
    }
  });

  it('python: three scripts that tell uv what they need, each opening the client', () => {
    const sections = sectionsFor('python', open);
    expect(sections.map((s) => s.title)).toEqual(['DataFrame', 'One sample at a time', 'Batches']);
    for (const section of sections) {
      expect(section.language).toBe('python');
      expect(section.code.startsWith('# /// script')).toBe(true);
      expect(section.code).toContain('async with SensAppClient("http://sensapp.example:3000") as client:');
    }
    expect(sections[0].code).toContain('frame.iter_slices(100_000)');
    expect(sections[2].code).toContain('SamplePoint(datetime.now(UTC)');
  });

  it('python: the client takes the token of the environment', () => {
    const code = codeOf('python', secured);
    expect(code).toContain('import os');
    expect(code).toContain('token=os.environ["SENSAPP_TOKEN"],');
  });

  it('telegraf: gives its token to Telegraf, which sends it the InfluxDB way', () => {
    expect(codeOf('telegraf', open)).toContain('urls = ["http://sensapp.example:3000"]');
    expect(codeOf('telegraf', open)).not.toContain('token');
    expect(codeOf('telegraf', secured)).toContain('token = "${SENSAPP_TOKEN}"');
    // SensApp reads the Token scheme of Telegraf: no header to rewrite
    expect(codeOf('telegraf', secured)).not.toContain('http_headers');
  });

  it('prometheus: writes and reads', () => {
    const code = codeOf('prometheus', open);
    expect(code).toContain('url: http://sensapp.example:3000/api/v1/prometheus_remote_write');
    expect(code).toContain('url: http://sensapp.example:3000/api/v1/prometheus_remote_read');
    expect(codeOf('prometheus', secured)).toContain('credentials_file');
  });

  it('curl: puts every address in quotes and the token in a header', () => {
    const code = codeOf('curl', secured);
    expect(code).toContain("'http://sensapp.example:3000/publish'");
    expect(code).toContain("'http://sensapp.example:3000/api/v2/write?org=sensapp&bucket=home&precision=s'");
    expect(code).toContain('-H "Authorization: Bearer $SENSAPP_TOKEN"');
  });
});
