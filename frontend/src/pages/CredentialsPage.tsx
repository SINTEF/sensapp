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
import { ADMIN_TOKEN_COMMAND, addSensor, tokenCommand, tokenScopes } from '../lib/credentials';
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
  { id: 'admin', help: 'Make tokens, nothing else. Only with the command line' },
] as const;

/** How many sensor names are offered to click, not to fill a page of a large server. */
const SUGGESTED_SENSORS = 40;

/** A part of the page, as on Load Data: a title and a sentence on the left, the content on the right. */
function Row({ title, description, children }: { title: string; description?: ReactNode; children: ReactNode }) {
  return (
    <section className="grid gap-x-8 gap-y-3 py-6 first:pt-4 lg:grid-cols-[minmax(0,20rem)_minmax(0,1fr)]">
      <div>
        <h2 className="text-base font-semibold">{title}</h2>
        {description && <p className="text-sm text-base-content/70 mt-1 leading-relaxed">{description}</p>}
      </div>
      <div className="flex flex-col gap-3 min-w-0">{children}</div>
    </section>
  );
}

const code = (text: string) => <code className="font-mono text-[0.8em] bg-base-200 px-0.5 py-0.5 rounded">{text}</code>;

function SensorsField({
  sensors,
  onChange,
  offered,
}: {
  sensors: string[];
  onChange: (sensors: string[]) => void;
  /** Names of the sensors of the server, to click in */
  offered: string[];
}) {
  const [draft, setDraft] = useState('');

  function add() {
    onChange(addSensor(sensors, draft));
    setDraft('');
  }

  const suggestions = offered.filter((name) => !sensors.includes(name));
  return (
    <div className="flex flex-col gap-2">
      <div className="flex gap-2">
        <input
          aria-label="Sensor name"
          autoComplete="off"
          spellCheck={false}
          placeholder="all of them"
          className="input input-bordered input-sm w-full font-mono"
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            // Enter adds the name, it does not submit the form
            if (event.key === 'Enter') {
              event.preventDefault();
              add();
            }
          }}
        />
        <button type="button" className="btn btn-sm" disabled={draft.trim() === ''} onClick={add}>
          Add
        </button>
      </div>

      {sensors.length > 0 && (
        <ul className="flex flex-wrap gap-1.5" aria-label="Allowed sensors">
          {sensors.map((name) => (
            <li key={name} className="badge badge-outline badge-lg gap-1.5 font-mono whitespace-pre">
              {name}
              <button
                type="button"
                className="cursor-pointer opacity-60 hover:opacity-100"
                aria-label={`Remove ${name}`}
                onClick={() => onChange(sensors.filter((item) => item !== name))}
              >
                ×
              </button>
            </li>
          ))}
        </ul>
      )}

      {suggestions.length > 0 && (
        <div className="flex flex-wrap gap-1.5" aria-label="Sensors of this server">
          {suggestions.slice(0, SUGGESTED_SENSORS).map((name) => (
            <button key={name} type="button" className="btn btn-quiet btn-xs font-mono" onClick={() => onChange(addSensor(sensors, name))}>
              {name}
            </button>
          ))}
          {suggestions.length > SUGGESTED_SENSORS && (
            <span className="text-xs text-base-content/50 self-center">and {suggestions.length - SUGGESTED_SENSORS} more</span>
          )}
        </div>
      )}
    </div>
  );
}

function CreatedTokenRow({ created, onDone }: { created: CreatedToken; onDone: () => void }) {
  const [copied, setCopied] = useState<boolean | null>(null);

  async function handleCopy() {
    setCopied(await copyText(created.token));
    window.setTimeout(() => setCopied(null), 2000);
  }

  return (
    <Row title="Your token" description="This is the only time it is shown: SensApp does not keep tokens. Copy it now.">
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
        <dd>{created.sensors?.length ? created.sensors.join(' · ') : 'all'}</dd>
        <dt className="text-base-content/60">Expires</dt>
        <dd>{new Date(created.expires_at * 1000).toLocaleString()}</dd>
        <dt className="text-base-content/60">Id</dt>
        <dd className="font-mono text-xs self-center break-all">{created.jti}</dd>
      </dl>

      <p className="text-sm text-base-content/70 leading-relaxed">
        Clients send it as {code('Authorization: Bearer')}. The code of{' '}
        <Link to="/load" className="link link-primary">
          Load Data
        </Link>{' '}
        reads it from {code('SENSAPP_TOKEN')}:
      </p>
      <CodeBlock code={`export SENSAPP_TOKEN=${created.token}\n`} language="bash" label="Export of the token" />

      <div>
        <button type="button" className="btn btn-ghost btn-sm" onClick={onDone}>
          Done
        </button>
      </div>
    </Row>
  );
}

/** Where a token is made when this page cannot: the command line, with the secret. */
function AuthenticationDisabled() {
  return (
    <Row title="Authentication is disabled" description="This server answers everyone, so nothing needs a token.">
      <p className="text-sm text-base-content/80 leading-relaxed">
        To require tokens, give SensApp a secret with {code('SENSAPP_JWT_SECRET')} ({code('sensapp generate-secret')} makes one) and remove{' '}
        {code('SENSAPP_AUTH_DISABLED')}.
      </p>
    </Row>
  );
}

/** What the page offers: the form, the command that makes the same token, and the button when the token in use is an admin's. */
function TokenMaker({ token, canRead, isAdmin }: { token: string | null; canRead: boolean; isAdmin: boolean }) {
  const metrics = useMetrics(undefined, { enabled: canRead });
  const openDialog = useAuthStore((state) => state.openDialog);

  const [subject, setSubject] = useState('');
  const [durationSeconds, setDuration] = useState<number>(DAY);
  const [scope, setScope] = useState<string[]>(['read', 'write']);
  const [sensors, setSensors] = useState<string[]>([]);

  const mutation = useMutation({
    mutationFn: async (body: Parameters<typeof createToken>[0]['body']) => unwrap(await createToken({ body })),
    // The token is not kept in the cache of the mutations once the page is left
    gcTime: 0,
  });

  const wish = { subject, scope, sensors, durationSeconds };
  const names = (metrics.data?.['dcat:dataset'] ?? []).map((dataset) => dataset['dct:title']);
  const wantsAdmin = scope.includes('admin');
  const ready = subject.trim() !== '' && scope.length > 0;

  function toggle(id: string) {
    setScope((current) => (current.includes(id) ? current.filter((item) => item !== id) : [...current, id]));
  }

  function handleSubmit(event: FormEvent) {
    event.preventDefault();
    if (!ready || wantsAdmin) return;
    mutation.mutate({ subject: subject.trim(), scope, sensors: sensors.length > 0 ? sensors : null, duration_seconds: durationSeconds });
  }

  const refused = mutation.error instanceof ApiError && mutation.error.isAuthError;

  return (
    <form onSubmit={handleSubmit} className="flex flex-col divide-y divide-base-300">
      {mutation.isSuccess && <CreatedTokenRow created={mutation.data} onDone={() => mutation.reset()} />}

      <Row title="Name" description="Who or what the token is for. It is the subject of the token, and in the logs of every request it makes.">
        <input
          name="subject"
          aria-label="Name"
          required
          maxLength={128}
          autoComplete="off"
          placeholder="edge-device-7"
          className="input input-bordered input-sm w-full"
          value={subject}
          onChange={(event) => setSubject(event.target.value)}
        />
      </Row>

      <Row
        title="Valid for"
        description="A token cannot be revoked: it works until it expires, or until the secret that signed it is rotated out. Prefer short."
      >
        <div className="join" role="radiogroup" aria-label="Valid for">
          {DURATIONS.map((item) => (
            <input
              key={item.seconds}
              type="radio"
              name="duration"
              aria-label={item.label}
              className="join-item btn btn-sm"
              checked={durationSeconds === item.seconds}
              onChange={() => setDuration(item.seconds)}
            />
          ))}
        </div>
      </Row>

      <Row title="What it may do" description="None of the scopes gives another.">
        <fieldset className="flex flex-col gap-1.5">
          <legend className="sr-only">Scopes</legend>
          {SCOPES.map((item) => (
            <label key={item.id} className="flex items-center gap-2 text-sm cursor-pointer">
              <input type="checkbox" className="checkbox checkbox-sm" checked={scope.includes(item.id)} onChange={() => toggle(item.id)} />
              <span className="font-mono">{item.id}</span>
              <span className="text-base-content/55">{item.help}</span>
            </label>
          ))}
        </fieldset>
      </Row>

      <Row title="Only these sensors" description="Names as the sensors have them. Left empty, the token reaches every sensor.">
        <SensorsField sensors={sensors} onChange={setSensors} offered={names} />
      </Row>

      <Row
        title="Command line"
        description={
          <>
            The same token, made where SensApp runs: it needs the secret. It is the only way to make an {code('admin')} token. In a container, put{' '}
            {code('docker exec <container>')} in front, on Kubernetes {code('kubectl exec deploy/<release> --')}.
          </>
        }
      >
        <CodeBlock code={tokenCommand(wish) + '\n'} language="bash" label="Command that makes the token" />
      </Row>

      <Row
        title="Make it here"
        description={isAdmin ? 'The token is shown once, at the top of the page.' : <>Needs a token with the {code('admin')} scope. It reads and writes nothing.</>}
      >
        {isAdmin ? (
          <>
            {mutation.isError && !refused && (
              <div role="alert" className="alert alert-error alert-soft py-2 text-sm">
                {mutation.error instanceof Error ? mutation.error.message : 'The token was not made'}
              </div>
            )}
            {wantsAdmin && <p className="text-sm text-base-content/70">An admin token is only made with the command line.</p>}
            <div>
              <button type="submit" className="btn btn-primary btn-sm" disabled={mutation.isPending || !ready || wantsAdmin}>
                {mutation.isPending ? 'Making…' : 'Make the token'}
              </button>
            </div>
          </>
        ) : (
          <>
            <p className="text-sm text-base-content/80 leading-relaxed">
              {token !== null ? 'The token in use does not have it. ' : ''}Make an admin token with the command line, it lasts an hour:
            </p>
            <CodeBlock code={ADMIN_TOKEN_COMMAND + '\n'} language="bash" label="Command that makes an admin token" />
            <div>
              <button type="button" className="btn btn-primary btn-sm" onClick={openDialog}>
                Use an admin token
              </button>
            </div>
          </>
        )}
      </Row>
    </form>
  );
}

/** Where an admin makes the tokens of the clients, and how anyone makes the admin token. */
export function CredentialsPage() {
  const token = useAuthStore((state) => state.token);
  // Without a token, the catalog tells whether the server asks for one. With one, it is not
  // asked here: an admin token cannot read, and the refusal would ask for a token again.
  const probe = useMetrics(undefined, { enabled: token === null });
  const scopes = token === null ? [] : tokenScopes(describeToken(token));

  let content: ReactNode;
  if (token === null && probe.isPending) {
    content = <Loading />;
  } else if (token === null && probe.isSuccess) {
    content = <AuthenticationDisabled />;
  } else {
    content = <TokenMaker token={token} canRead={scopes.includes('read')} isAdmin={scopes.includes('admin')} />;
  }

  return (
    <div className="flex flex-col gap-3 px-1 sm:px-2 pb-8">
      {content}
    </div>
  );
}

export default CredentialsPage;
