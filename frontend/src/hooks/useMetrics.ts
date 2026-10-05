import { useQuery } from '@tanstack/react-query';
import { listMetrics } from '../client';
import { unwrap } from '../api/clientConfig';

export interface DcatDataset {
  '@type': string;
  '@id': string;
  'dct:identifier': string;
  'dct:title': string;
  'dct:description': string;
  'dcat:keyword': string[];
  'sensor:type': string;
  'sensor:seriesCount'?: number;
  'sensor:labelDimensions'?: string[];
  'sensor:unit'?: string;
  'dcat:distribution': Array<{
    '@type': string;
    'dcat:accessURL'?: string;
    'dcat:downloadURL'?: string;
    'dcat:mediaType': string;
  }>;
}

export interface DcatCatalog {
  '@context': Record<string, string>;
  '@type': string;
  '@id': string;
  'dct:title': string;
  'dct:description': string;
  'dct:publisher': {
    '@type': string;
    'foaf:name': string;
  };
  'dcat:dataset': DcatDataset[];
}

export function useMetrics(
  filters?: {
    name?: string;
    nameRegex?: string;
    type?: string;
  },
  // `false` when the catalog is not needed, nor worth a refusal that asks for a token
  options?: { enabled?: boolean },
) {
  return useQuery({
    queryKey: ['metrics', filters],
    enabled: options?.enabled ?? true,
    queryFn: async () => {
      const result = await listMetrics({
        query: {
          name: filters?.name,
          name_regex: filters?.nameRegex,
          type: filters?.type,
        },
      });
      return unwrap(result) as unknown as DcatCatalog;
    },
  });
}
