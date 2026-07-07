// services/monday.service.js
// Monday.com HTTP client with retry/backoff logic.

import { MONDAY_API_TOKEN } from '../config.js';

const SLEEP           = ms => new Promise(r => setTimeout(r, ms));
const MAX_RETRIES     = 6;
const BASE_BACKOFF_MS = 1000;

export const BATCH_SIZE        = 10;
export const BATCH_CONCURRENCY = 1;

export async function makeMondayApiRequest(query) {
  let attempt = 0;
  while (true) {
    try {
      const response = await fetch('https://api.monday.com/v2', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${MONDAY_API_TOKEN}` },
        body: JSON.stringify({ query }),
      });

      const json = await response.json().catch(() => ({}));

      if (!response.ok) {
        const retryable = [429, 502, 503, 504].includes(response.status);
        if (retryable && attempt < MAX_RETRIES) { await SLEEP(BASE_BACKOFF_MS * 2 ** attempt++); continue; }
        throw new Error(`HTTP ${response.status}: ${JSON.stringify(json)}`);
      }

      if (json.errors?.length) {
        const msg       = JSON.stringify(json.errors);
        const transient = /rate|complexity|timeout|temporary|lock/i.test(msg);
        if (transient && attempt < MAX_RETRIES) {
          await SLEEP(BASE_BACKOFF_MS * 2 ** attempt++ + Math.floor(Math.random() * 500));
          continue;
        }
        throw new Error(`GraphQL errors: ${msg}`);
      }

      return json;
    } catch (err) {
      if (attempt < MAX_RETRIES) { await SLEEP(BASE_BACKOFF_MS * 2 ** attempt++); continue; }
      throw err;
    }
  }
}

export function buildAliasedMutation(ops) {
  return `mutation{\n${ops.join('\n')}\n}`;
}

export function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export async function runBatches(batchQueries, concurrency = BATCH_CONCURRENCY) {
  let i = 0;
  const workers = new Array(Math.min(concurrency, batchQueries.length)).fill(0).map(async () => {
    while (i < batchQueries.length) await makeMondayApiRequest(batchQueries[i++]);
  });
  await Promise.all(workers);
}