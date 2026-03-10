import { useQuery } from '@tanstack/react-query';
import { getSeriesData } from '../client';
import { extractErrorMessage } from '../api/clientConfig';

export interface SenMLRecord {
  bn?: string;
  bt?: number;
  bu?: string;
  n?: string;
  t?: number;
  u?: string;
  v?: number;
  vs?: string;
  vb?: boolean;
  vd?: string;
}

export function useSeriesData(
  seriesUuid: string | undefined,
  options?: {
    start?: string;
    end?: string;
    format?: string;
    limit?: number;
  }
) {
  return useQuery({
    queryKey: ['seriesData', seriesUuid, options],
    queryFn: async () => {
      const result = await getSeriesData({
        path: { series_uuid: seriesUuid! },
        query: {
          format: options?.format || 'senml',
          start: options?.start,
          end: options?.end,
          limit: options?.limit,
        },
      });
      if (result.error) {
        throw new Error(extractErrorMessage(result.error));
      }
      return result.data as unknown as SenMLRecord[];
    },
    enabled: !!seriesUuid,
  });
}
