import { expect, it } from 'vitest';
import { createClient } from '../client/client';
import { listSeries, readiness } from '../client/sdk.gen';

it('sends a generated catalog request and parses the response', async () => {
  let requestedUrl = '';
  const client = createClient({
    baseUrl: 'http://example.test',
    fetch: async (input) => {
      requestedUrl = input instanceof Request ? input.url : String(input);
      return new Response(JSON.stringify({ series: [] }), {
        headers: { 'Content-Type': 'application/json' },
      });
    },
  });

  const result = await listSeries({ client, query: { metric: 'temperature' } });

  expect(requestedUrl).toBe('http://example.test/series?metric=temperature');
  expect(result.data).toEqual({ series: [] });
});

const liveUrl = import.meta.env.VITE_SENSAPP_LIVE_URL;

it.skipIf(!liveUrl)('works against a running SensApp API', async () => {
  const client = createClient({ baseUrl: liveUrl });

  const ready = await readiness({ client, throwOnError: true });
  const catalog = await listSeries({ client, throwOnError: true });

  expect(ready.response.status).toBe(200);
  expect(catalog.response.status).toBe(200);
});
