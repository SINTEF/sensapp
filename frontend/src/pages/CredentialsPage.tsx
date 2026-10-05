import { useState } from 'react';
import type { FormEvent, ReactNode } from 'react';
import { useMutation } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { createToken } from '../client';
import type { CreatedToken } from '../client';
import { ApiError, unwrap } from '../api/clientConfig';
import { CodeBlock } from '../components/CodeBlock';
import { Loading } from '../components/Feedback';
import { useMetrics } from '../hooks/useMetrics';
import { copyText } from '../lib/copyText';
import { ADMIN_TOKEN_COMMAND, parseSensorNames, tokenScopes } from '../lib/credentials';
import { describeToken, useAuthStore } from '../stores/useAuthStore';

const DAY = 24 * 3600;
const DURATIONS = [
  { label: '1 hour', seconds: 3600 },
  { label: '1 day', seconds: DAY },
  { label: '30 days', seconds: 30 * DAY },
  { label: '1 year', seconds: 365 * DAY },
] as const;

const SCOPES = [
  { id: 'read', help: 'Read the series and the samples' },
  { id: 'write', help: 'Send samples' },
  { id: 'delete', help: 'Delete series and samples, and run the maintenance' },
] as const;

/** How many sensor names are offered to click, not to fill a page of a large server. */
const SUGGESTED_SENSORS = 40;

function Card({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="bg-base-100 rounded-lg border border-base-300 shadow-sm">
      <h2 className="text-sm font-semibold px-4 py-2 border-b border-base-300">{title}</h2>
      <div className="p-4 flex flex-col gap-3">{children}</div>
    </section>
  );
}

/** The command that makes an admin token, and the way to give it to the UI. */
function NeedAdmin({ signedIn }: { signedIn: boolean }) {
  const openDialog = useAuthStore((state) => state.openDialog);
  return (
    <Card title="An admin token is needed">
      <p className="text-sm text-base-content/80 leading-relaxed">
        {signedIn ? 'The token in use does not have the ' : 'Making a token needs a token with the '}
        <code className="font-mono">admin</code> scope. It gives nothing else: it cannot read nor write data. Make one where SensApp runs, with the secret:
      </p>
      <CodeBlock code={ADMIN_TOKEN_COMMAND + '\n'} language="bash" label="Command that makes an admin token" />
      <p className="text-sm text-base-content/70 leading-relaxed">
        An admin token lasts an hour unless asked otherwise; make another when it expires. On a local run without a secret, the token SensApp
        prints when it starts is one.
      </p>
      <div>
        <button type="button" className="btn btn-primary btn-sm" onClick={openDialog}>
          Use an admin token
        </button>
      </div>
    </Card>
  );
}

function AuthenticationDisabled() {
  return (
    <Card title="Authentication is disabled">
      <p className="text-sm text-base-content/80 leading-relaxed">
        This server answers everyone, so nothing needs a token. To require tokens, give SensApp a secret with{' '}
        <code className="font-mono">SENSAPP_JWT_SECRET</code> (<code className="font-mono">sensapp generate-secret</code> makes one) and remove{' '}
        <code className="font-mono">SENSAPP_AUTH_DISABLED</code>.
      </p>
    </Card>
  );
}

function SensorChoices({ names, chosen, onAdd }: { names: string[]; chosen: string[]; onAdd: (name: string) => void }) {
  const offered = names.filter((name) => !chosen.includes(name)).slice(0, SUGGESTED_SENSORS);
  if (offered.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-1.5" aria-label="Sensors of this server">
      {offered.map((name) => (
        <button key={name} type="button" className="btn btn-quiet btn-xs font-mono" onClick={() => onAdd(name)}>
          {name}
        </button>
      ))}
      {names.length > SUGGESTED_SENSORS && <span className="text-xs text-base-content/50 self-center">and {names.length - SUGGESTED_SENSORS} more</span>}
    </div>
  );
}

function CreatedTokenView({ created, onAnother }: { created: CreatedToken; onAnother: () => void }) {
  const [copied, setCopied] = useState<boolean | null>(null);

  async function handleCopy() {
    setCopied(await copyText(created.token));
    window.setTimeout(() => setCopied(null), 2000);
  }

  return (
    <Card title="Your token">
      <div role="status" className="alert alert-warning alert-soft py-2 text-sm">
        This is the only time the token is shown: SensApp does not keep it. Copy it now.
      </div>

      <div className="flex gap-2">
        <input
          readOnly
          aria-label="Token"
          value={created.token}
          className="input input-bordered input-sm w-full font-mono text-xs"
          onFocus={(event) => event.currentTarget.select()}
        />
        <button type="button" className="btn btn-primary btn-sm min-w-16" onClick={() => void handleCopy()}>
          {copied === true ? 'Copied' : copied === false ? 'Failed' : 'Copy'}
        </button>
      </div>

      <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-sm">
        <dt className="text-base-content/60">For</dt>
        <dd>{created.subject}</dd>
        <dt className="text-base-content/60">Scope</dt>
        <dd>{created.scope.join(', ')}</dd>
        <dt className="text-base-content/60">Sensors</dt>
        <dd>{created.sensors?.length ? created.sensors.join(', ') : 'all'}</dd>
        <dt className="text-base-content/60">Expires</dt>
        <dd>{new Date(created.expires_at * 1000).toLocaleString()}</dd>
        <dt className="text-base-content/60">Id</dt>
        <dd className="font-mono text-xs self-center break-all">{created.jti}</dd>
      </dl>

      <p className="text-sm text-base-content/70 leading-relaxed">
        Clients take it as <code className="font-mono">Authorization: Bearer</code>. The code of{' '}
        <Link to="/load" className="link">
          Load Data
        </Link>{' '}
        reads it from <code className="font-mono">SENSAPP_TOKEN</code>:
      </p>
      <CodeBlock code={`export SENSAPP_TOKEN=${created.token}\n`} language="bash" label="Export of the token" />

      <div>
        <button type="button" className="btn btn-ghost btn-sm" onClick={onAnother}>
          Make another token
        </button>
      </div>
    </Card>
  );
}

/** `canRead`: the token may read the catalog, which gives the sensors to choose from. An admin token alone cannot. */
function TokenForm({ canRead }: { canRead: boolean }) {
  const metrics = useMetrics(undefined, { enabled: canRead });
  const [subject, setSubject] = useState('');
  const [scope, setScope] = useState<string[]>(['read', 'write']);
  const [sensorsText, setSensorsText] = useState('');
  const [duration, setDuration] = useState<number>(DAY);

  const mutation = useMutation({
    mutationFn: async (body: Parameters<typeof createToken>[0]['body']) => unwrap(await createToken({ body })),
    // The token is not kept in the cache of the mutations once the page is left
    gcTime: 0,
  });

  const sensors = parseSensorNames(sensorsText);
  const names = (metrics.data?.['dcat:dataset'] ?? []).map((dataset) => dataset['dct:title']);

  if (mutation.isSuccess) {
    return <CreatedTokenView created={mutation.data} onAnother={() => mutation.reset()} />;
  }

  function toggle(id: string) {
    setScope((current) => (current.includes(id) ? current.filter((item) => item !== id) : [...current, id]));
  }

  function handleSubmit(event: FormEvent) {
    event.preventDefault();
    mutation.mutate({
      subject: subject.trim(),
      scope,
      sensors: sensors.length > 0 ? sensors : null,
      duration_seconds: duration,
    });
  }

  return (
    <Card title="Make a token">
      <form className="flex flex-col gap-4" onSubmit={handleSubmit}>
        <label className="flex flex-col gap-1">
          <span className="text-xs font-medium text-base-content/60">Name</span>
          <input
            name="subject"
            required
            maxLength={128}
            autoComplete="off"
            placeholder="edge-device-7"
            className="input input-bordered input-sm w-full"
            value={subject}
            onChange={(event) => setSubject(event.target.value)}
          />
          <span className="text-xs text-base-content/55">Who or what it is for. It is in the logs of every request the token makes.</span>
        </label>

        <fieldset className="flex flex-col gap-1.5">
          <legend className="text-xs font-medium text-base-content/60 mb-1">What it may do</legend>
          {SCOPES.map((item) => (
            <label key={item.id} className="flex items-center gap-2 text-sm cursor-pointer">
              <input type="checkbox" className="checkbox checkbox-sm" checked={scope.includes(item.id)} onChange={() => toggle(item.id)} />
              <span className="font-mono">{item.id}</span>
              <span className="text-base-content/55">{item.help}</span>
            </label>
          ))}
        </fieldset>

        <div className="flex flex-col gap-1.5">
          <label className="flex flex-col gap-1">
            <span className="text-xs font-medium text-base-content/60">Only these sensors</span>
            <input
              name="sensors"
              autoComplete="off"
              spellCheck={false}
              placeholder="all of them"
              className="input input-bordered input-sm w-full font-mono"
              value={sensorsText}
              onChange={(event) => setSensorsText(event.target.value)}
            />
            <span className="text-xs text-base-content/55">Names separated by commas. Left empty, the token reaches every sensor.</span>
          </label>
          <SensorChoices
            names={names}
            chosen={sensors}
            onAdd={(name) => setSensorsText((text) => (text.trim() === '' ? name : `${text.trim().replace(/,$/, '')}, ${name}`))}
          />
        </div>

        <label className="flex flex-col gap-1">
          <span className="text-xs font-medium text-base-content/60">Valid for</span>
          <select className="select select-bordered select-sm w-full max-w-xs" value={duration} onChange={(event) => setDuration(Number(event.target.value))}>
            {DURATIONS.map((item) => (
              <option key={item.seconds} value={item.seconds}>
                {item.label}
              </option>
            ))}
          </select>
          <span className="text-xs text-base-content/55">
            A token cannot be revoked: it works until then, or until the secret that signed it is rotated out. Prefer short.
          </span>
        </label>

        {mutation.isError && !(mutation.error instanceof ApiError && mutation.error.isAuthError) && (
          <div role="alert" className="alert alert-error alert-soft py-2 text-sm">
            {mutation.error instanceof Error ? mutation.error.message : 'The token was not made'}
          </div>
        )}

        <div>
          <button type="submit" className="btn btn-primary btn-sm" disabled={mutation.isPending || subject.trim() === '' || scope.length === 0}>
            {mutation.isPending ? 'Making…' : 'Make the token'}
          </button>
        </div>
      </form>
    </Card>
  );
}

/** Where an admin makes the tokens of the clients, and how anyone makes the admin token. */
export function CredentialsPage() {
  const token = useAuthStore((state) => state.token);
  // Without a token, the catalog tells whether the server asks for one. With one, it is not
  // asked: an admin token cannot read, and the refusal would ask for a token again.
  const metrics = useMetrics(undefined, { enabled: token === null });
  const scopes = token === null ? [] : tokenScopes(describeToken(token));
  const isAdmin = scopes.includes('admin');

  let content;
  if (isAdmin) {
    content = <TokenForm canRead={scopes.includes('read')} />;
  } else if (token !== null) {
    content = <NeedAdmin signedIn />;
  } else if (metrics.isPending) {
    content = <Loading />;
  } else if (metrics.isSuccess) {
    content = <AuthenticationDisabled />;
  } else {
    content = <NeedAdmin signedIn={false} />;
  }

  return <div className="flex flex-col gap-3 px-1 sm:px-2 pb-8 max-w-2xl">{content}</div>;
}

export default CredentialsPage;
