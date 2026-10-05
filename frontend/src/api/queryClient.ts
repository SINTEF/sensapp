import { MutationCache, QueryCache, QueryClient } from '@tanstack/react-query';
import { ApiError } from './clientConfig';
import { useAuthStore } from '../stores/useAuthStore';

/** The server refused the request: ask for a token instead of showing an error alone. */
function askForTokenWhenRefused(error: unknown) {
  if (error instanceof ApiError && error.isAuthError) {
    useAuthStore.getState().reject(error.message);
  }
}

export function createQueryClient() {
  return new QueryClient({
    queryCache: new QueryCache({ onError: askForTokenWhenRefused }),
    // Making a token is a mutation, and refused like a read when the token is not an admin's
    mutationCache: new MutationCache({ onError: askForTokenWhenRefused }),
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
