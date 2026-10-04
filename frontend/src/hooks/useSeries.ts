import { useQuery } from '@tanstack/react-query';
import { listSeries } from '../client';
import { unwrap } from '../api/clientConfig';

export interface SeriesLabel {
  [key: string]: string;
}

export interface SeriesDataset {
  '@type': string;
  '@id': string;
  'dct:identifier': string;
  'dct:title': string;
  'dct:description': string;
  'sensor:type': string;
  'sensor:unit'?: string;
  'sensor:labels'?: SeriesLabel[];
  'dcat:distribution': Array<{
    '@type'?: string;
    'dcat:downloadURL': string;
    'dcat:mediaType': string;
  }>;
}

export interface SeriesCatalog {
  '@context': Record<string, string>;
  '@type': string;
  '@id': string;
  'dct:title': string;
  'dct:description': string;
  'dcat:dataset': SeriesDataset[];
  'hydra:view'?: {
    '@type': string;
    'hydra:next': string;
    'hydra:itemsPerPage': number;
  };
}

export function useSeries(filters?: {
  metric?: string;
  selector?: string;
}) {
  return useQuery({
    queryKey: ['series', filters],
    queryFn: async () => {
      const result = await listSeries({
        query: {
          metric: filters?.metric,
          selector: filters?.selector,
        },
      });
      return unwrap(result) as unknown as SeriesCatalog;
    },
    enabled: !!filters?.metric || !!filters?.selector,
  });
}
