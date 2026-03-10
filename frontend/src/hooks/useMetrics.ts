import { useQuery } from '@tanstack/react-query';
import { listMetrics } from '../client';
import { extractErrorMessage } from '../api/clientConfig';

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

export function useMetrics(filters?: {
  name?: string;
  nameRegex?: string;
  type?: string;
}) {
  return useQuery({
    queryKey: ['metrics', filters],
    queryFn: async () => {
      const result = await listMetrics({
        query: {
          name: filters?.name,
          name_regex: filters?.nameRegex,
          type: filters?.type,
        },
      });
      if (result.error) {
        throw new Error(extractErrorMessage(result.error));
      }
      return result.data as unknown as DcatCatalog;
    },
  });
}
