// services/samsara.service.js
// Fetches trailer asset data from the Samsara API.

import { SAMSARA_API_TOKEN, SAMSARA_BASE_URL } from '../config.js';

export async function fetchSamsaraTrailers() {
  const response = await fetch(SAMSARA_BASE_URL, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${SAMSARA_API_TOKEN}`,
    },
  });

  if (!response.ok)
    throw new Error(`Samsara API: HTTP ${response.status} ${response.statusText}`);

  const data = await response.json();
  return data?.assets ?? [];
}