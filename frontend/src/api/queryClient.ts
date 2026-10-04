import { QueryCache, QueryClient } from '@tanstack/react-query';
import { ApiError } from './clientConfig';
import { useAuthStore } from '../stores/useAuthStore';

export function createQueryClient() {
  return new QueryClient({
    // The server refused the request: ask for a token instead of showing an error alone
    queryCache: new QueryCache({
      onError: (error) => {
        if (error instanceof ApiError && error.isAuthError) {
          useAuthStore.getState().reject(error.message);
        }
      },
    }),
    defaultOptions: {
      queries: {
        staleTime: 30_000,
        // Asking again does not make a token valid
        retry: (failureCount, error) =>
          !(error instanceof ApiError && error.isAuthError) && failureCount < 1,
      },
    },
  });
}
