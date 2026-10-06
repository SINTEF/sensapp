import { afterEach, describe, expect, it, vi } from 'vitest';
import { downloadQuery, fallbackFileName, fileNameFrom, saveBlob } from './download';
import type { DownloadChoice } from './download';

const RANGE = { start: '2026-10-06T09:00:00.000Z', end: '2026-10-06T10:00:00.000Z' };
const RAW: DownloadChoice = { format: 'csv', window: 'chart', sampling: 'raw', step: '5m', aggregation: 'max' };

describe('downloadQuery', () => {
  it('asks the window of the chart, raw', () => {
    expect(downloadQuery({ type: 'float' }, RAW, RANGE)).toEqual({
      format: 'csv',
      download: true,
      start: RANGE.start,
      end: RANGE.end,
    });
  });

  it('asks every sample without a window', () => {
    expect(downloadQuery({ type: 'float' }, { ...RAW, window: 'all' }, RANGE)).toEqual({ format: 'csv', download: true });
  });

  it('aggregates numbers only', () => {
    const aggregated: DownloadChoice = { ...RAW, format: 'arrow', sampling: 'aggregated' };
    expect(downloadQuery({ type: 'integer' }, aggregated, RANGE)).toMatchObject({ format: 'arrow', step: '5m', aggregation: 'max' });
    for (const type of ['string', 'boolean', 'location', 'json']) {
      const query = downloadQuery({ type }, aggregated, RANGE);
      expect(query).not.toHaveProperty('step');
      expect(query).not.toHaveProperty('aggregation');
    }
  });
});

describe('fileNameFrom', () => {
  it('prefers the UTF-8 name', () => {
    expect(
      fileNameFrom(`attachment; filename="Sanit_ranlegg.csv"; filename*=UTF-8''Sanit%C3%A6ranlegg%20value.csv`),
    ).toBe('Sanitæranlegg value.csv');
  });

  it('takes the plain name, quoted or not', () => {
    expect(fileNameFrom('attachment; filename="cpu_host-a.csv"')).toBe('cpu_host-a.csv');
    expect(fileNameFrom('attachment; filename=cpu.jsonl')).toBe('cpu.jsonl');
  });

  it('takes the plain name when the UTF-8 one is broken', () => {
    expect(fileNameFrom(`attachment; filename="a.csv"; filename*=UTF-8''%E0%A4%A.csv`)).toBe('a.csv');
  });

  it('has nothing without a name', () => {
    expect(fileNameFrom(null)).toBeUndefined();
    expect(fileNameFrom('attachment')).toBeUndefined();
    expect(fileNameFrom('attachment; filename=""')).toBeUndefined();
  });
});

describe('fallbackFileName', () => {
  it('is the name of the series with the extension of the format', () => {
    expect(fallbackFileName({ name: 'cpu' }, 'senml')).toBe('cpu.json');
    expect(fallbackFileName({ name: '' }, 'arrow')).toBe('series.arrow');
  });
});

describe('saveBlob', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('clicks a link to the data, with the file name, then lets the data go', () => {
    vi.useFakeTimers();
    // jsdom has no object URLs
    const createObjectURL = vi.fn(() => 'blob:file');
    const revokeObjectURL = vi.fn();
    Object.assign(URL, { createObjectURL, revokeObjectURL });
    const click = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (this: HTMLAnchorElement) {
      expect(this.download).toBe('cpu.csv');
      expect(this.href).toBe('blob:file');
    });

    saveBlob(new Blob(['timestamp,value\n']), 'cpu.csv');
    expect(click).toHaveBeenCalledTimes(1);
    expect(document.querySelector('a[download]')).toBeNull();
    expect(revokeObjectURL).not.toHaveBeenCalled();
    vi.runAllTimers();
    expect(revokeObjectURL).toHaveBeenCalledWith('blob:file');
  });
});
