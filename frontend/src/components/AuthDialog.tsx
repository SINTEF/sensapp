import { useState } from 'react';
import type { FormEvent } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { normalizeToken, useAuthStore } from '../stores/useAuthStore';

/** Asks for a JWT when the server refuses a request (401, or 403 for a token that is not enough). */
export function AuthDialog() {
  const { dialogOpen, message, token, signIn, closeDialog } = useAuthStore();
  const queryClient = useQueryClient();
  const [input, setInput] = useState('');

  if (!dialogOpen) return null;

  function handleSubmit(event: FormEvent) {
    event.preventDefault();
    const candidate = normalizeToken(input);
    if (!candidate) return;
    signIn(candidate);
    setInput('');
    // Everything that was refused is asked again with the token
    void queryClient.invalidateQueries();
  }

  return (
    <div
      className="modal modal-open"
      role="dialog"
      aria-modal="true"
      aria-labelledby="auth-dialog-title"
      onKeyDown={(event) => event.key === 'Escape' && closeDialog()}
    >
      <form className="modal-box max-w-md" onSubmit={handleSubmit}>
        <h2 id="auth-dialog-title" className="text-lg font-bold">
          {token ? 'This token is not accepted' : 'Authentication required'}
        </h2>
        <p className="py-2 text-sm text-base-content/70">
          This SensApp asks for a token. Paste one generated with:
        </p>
        <pre className="bg-base-200 rounded px-3 py-2 text-xs overflow-x-auto">
          <code>sensapp generate-token ui --scope read</code>
        </pre>

        {message && (
          <div role="alert" className="alert alert-warning alert-soft mt-3 py-2 text-sm">
            {message}
          </div>
        )}

        <label className="form-control mt-3 block">
          <span className="text-xs font-medium text-base-content/60">JWT</span>
          <input
            type="password"
            name="token"
            autoFocus
            autoComplete="off"
            spellCheck={false}
            placeholder="eyJ…"
            className="input input-bordered w-full font-mono text-xs"
            value={input}
            onChange={(event) => setInput(event.target.value)}
          />
        </label>
        <p className="mt-1 text-xs text-base-content/40">
          Kept for this browser tab only, and sent to this server only.
        </p>

        <div className="modal-action">
          <button type="button" className="btn btn-ghost btn-sm" onClick={closeDialog}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary btn-sm" disabled={!normalizeToken(input)}>
            Use token
          </button>
        </div>
      </form>
      <button
        type="button"
        className="modal-backdrop"
        aria-label="Close"
        onClick={closeDialog}
      />
    </div>
  );
}
