import type { SeriesInfo } from '../stores/useSelectionStore';
import { isNumericType } from './chartStep';
import type { Aggregation } from './chartStep';
import type { TimeRange } from './timeRange';

/** What `GET /series/{uuid}` can export, with the extension the server gives the file. */
export const DOWNLOAD_FORMATS = [
  { id: 'csv', label: 'CSV', extension: 'csv' },
  { id: 'jsonl', label: 'JSON Lines', extension: 'jsonl' },
  { id: 'senml', label: 'SenML', extension: 'json' },
  { id: 'arrow', label: 'Arrow', extension: 'arrow' },
] as const;
export type DownloadFormat = (typeof DOWNLOAD_FORMATS)[number]['id'];

/** What the download dialog asks. */
export interface DownloadChoice {
  format: DownloadFormat;
  /** The window of the chart, or every sample of the series */
  window: 'chart' | 'all';
  /** The samples as they are, or one per step */
  sampling: 'raw' | 'aggregated';
  step: string;
  aggregation: Aggregation;
}

/**
 * The query of `GET /series/{uuid}` for a download. Only numbers are aggregated, as on the chart: the
 * others are downloaded raw.
 */
export function downloadQuery(series: Pick<SeriesInfo, 'type'>, choice: DownloadChoice, range: TimeRange) {
  return {
    format: choice.format,
    download: true,
    ...(choice.window === 'chart' ? { start: range.start, end: range.end } : {}),
    ...(choice.sampling === 'aggregated' && isNumericType(series.type)
      ? { step: choice.step, aggregation: choice.aggregation }
      : {}),
  };
}

/** The file name of a `Content-Disposition`: the UTF-8 `filename*` when there is one, else `filename`. */
export function fileNameFrom(contentDisposition: string | null | undefined): string | undefined {
  if (!contentDisposition) return undefined;
  const extended = /filename\*\s*=\s*UTF-8''([^;\s]+)/i.exec(contentDisposition);
  if (extended) {
    try {
      return decodeURIComponent(extended[1]);
    } catch {
      // A broken encoding: the plain name below
    }
  }
  const plain = /filename\s*=\s*(?:"([^"]*)"|([^;\s]+))/i.exec(contentDisposition);
  return plain?.[1] || plain?.[2] || undefined;
}

/** The name of a file when the server gives none. */
export function fallbackFileName(series: Pick<SeriesInfo, 'name'>, format: DownloadFormat): string {
  const extension = DOWNLOAD_FORMATS.find((f) => f.id === format)!.extension;
  return `${series.name || 'series'}.${extension}`;
}

/** Saves the file as the browser saves a download: a link to the data, clicked. */
export function saveBlob(blob: Blob, fileName: string) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = fileName;
  link.hidden = true;
  document.body.append(link);
  link.click();
  link.remove();
  // Some browsers read the URL after the click has returned
  window.setTimeout(() => URL.revokeObjectURL(url), 10_000);
}
