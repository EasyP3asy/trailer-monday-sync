// services/skybitz-xml.service.js
// Fetches trailer positions from the SkyBitz XML API.

import { XMLParser } from 'fast-xml-parser';
import { SKYBITZ_BASE, SKYBITZ_CUSTOMER, SKYBITZ_PASSWORD, SKYBITZ_VERSION } from '../config.js';

export async function fetchSkybitzPositions() {
  const url = `${SKYBITZ_BASE}/QueryPositions?assetid=ALL` +
    `&customer=${encodeURIComponent(SKYBITZ_CUSTOMER)}` +
    `&password=${encodeURIComponent(SKYBITZ_PASSWORD)}` +
    `&version=${encodeURIComponent(SKYBITZ_VERSION)}`;

  const r = await fetch(url, { method: 'GET' });
  if (!r.ok) throw new Error(`SkyBitz XML API: HTTP ${r.status} ${await r.text()}`);

  const xml = await r.text();
  const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '' });
  return parser.parse(xml);
}