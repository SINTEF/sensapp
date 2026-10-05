import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it } from 'vitest';
import { ChartOptions } from './ChartOptions';
import { useSelectionStore } from '../stores/useSelectionStore';

describe('ChartOptions', () => {
  beforeEach(() => useSelectionStore.setState({ chartStyle: 'line', logScale: false }));

  it('chooses the style', async () => {
    const user = userEvent.setup();
    render(<ChartOptions />);
    const style = screen.getByRole('combobox', { name: 'Style' });
    expect(style).toHaveValue('line');
    expect(screen.getAllByRole('option').map((o) => o.textContent)).toEqual(['line', 'step', 'area', 'stacked', 'bars']);

    await user.selectOptions(style, 'stacked');
    expect(useSelectionStore.getState().chartStyle).toBe('stacked');
  });

  it('switches the log scale on and off', async () => {
    const user = userEvent.setup();
    render(<ChartOptions />);
    const log = screen.getByRole('button', { name: 'log' });
    expect(log).toHaveAttribute('aria-pressed', 'false');

    await user.click(log);
    expect(useSelectionStore.getState().logScale).toBe(true);
    expect(log).toHaveAttribute('aria-pressed', 'true');
    await user.click(log);
    expect(useSelectionStore.getState().logScale).toBe(false);
  });
});
