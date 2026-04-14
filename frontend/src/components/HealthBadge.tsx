import { useQuery } from '@tanstack/react-query';
import { readiness } from '../client';
import type { ReadinessResponse } from '../client';

export function HealthBadge() {
  const { data, isError } = useQuery({
    queryKey: ['health'],
    queryFn: async () => {
      const result = await readiness();
      if (result.error) {
        throw new Error('Backend unreachable');
      }
      return result.data as ReadinessResponse;
    },
    refetchInterval: 30000,
    retry: false,
  });

  const isHealthy = data?.status === 'ready' && !isError;

  return (
    <div className="flex items-center gap-1.5" title={isHealthy ? `Database: ${data?.database}` : 'Backend disconnected'}>
      <span className={`inline-block w-2 h-2 rounded-full ${isHealthy ? 'bg-success animate-pulse' : 'bg-error'}`} />
      <span className="text-xs font-medium opacity-70">
        {isHealthy ? 'Connected' : 'Disconnected'}
      </span>
    </div>
  );
}
