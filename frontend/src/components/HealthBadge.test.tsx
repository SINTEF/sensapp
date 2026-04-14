import { render, screen, waitFor } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { HealthBadge } from '../components/HealthBadge';

const mockReadiness = vi.fn();
vi.mock('../client', () => ({
  readiness: (...args: unknown[]) => mockReadiness(...args),
}));

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
  };
}

describe('HealthBadge', () => {
  it('shows "Connected" when backend is healthy', async () => {
    mockReadiness.mockResolvedValue({
      data: { status: 'ready', database: 'sqlite' },
    });

    render(<HealthBadge />, { wrapper: createWrapper() });
    expect(await screen.findByText('Connected')).toBeInTheDocument();
  });

  it('shows "Disconnected" when backend returns error', async () => {
    mockReadiness.mockResolvedValue({
      data: undefined,
      error: { status: 'error' },
    });

    render(<HealthBadge />, { wrapper: createWrapper() });
    await waitFor(() => {
      expect(screen.getByText('Disconnected')).toBeInTheDocument();
    });
  });

  it('shows "Disconnected" on network failure', async () => {
    mockReadiness.mockRejectedValue(new Error('Network error'));

    render(<HealthBadge />, { wrapper: createWrapper() });
    await waitFor(() => {
      expect(screen.getByText('Disconnected')).toBeInTheDocument();
    });
  });
});
