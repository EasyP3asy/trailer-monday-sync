// skybitz-parser.js
//
// Pure functions only: no network calls, no cookie jar, no side effects.
// String/object in, string/object out. This is the easiest file to unit
// test in isolation, since you can feed it sample HTML/data without ever
// hitting SkyBitz's real servers.

import { load } from "cheerio";

// ---- Encode a plain object as application/x-www-form-urlencoded ----
export function toFormUrlEncoded(data) {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(data)) {
    params.append(k, v == null ? "" : String(v));
  }
  return params.toString();
}

// ---- Parse the LAABSearch results table out of raw HTML ----
export function parseAssets(html) {
  const $ = load(html);

  // 1) Find the table that contains the asset headers
  const table = $("table")
    .filter((i, el) => {
      const t = $(el).text();
      return /Asset ID/i.test(t) && /Time of Observation/i.test(t);
    })
    .first();

    

  if (!table.length) {
    throw new Error("Could not find asset results table in HTML.");
  }

  // 2) Get all text from the table and turn it into cleaned lines
  let lines = table
    .text()
    .split(/\r?\n/)
    .map((l) => l.replace(/\s+/g, " ").trim())
    .filter((l) => l.length > 0);




  // 3) Find the index of the last header label ("Battery Status")
  const idxBattery = lines.findIndex((l) => /Battery Status/i.test(l));
  if (idxBattery === -1) {
    throw new Error("Could not find Battery Status label in table text.");
  }

  // Everything after this are data lines
  const start = idxBattery + 1;

  // Each asset is 13 consecutive lines in fixed order:
  // 0 msgId, 1 time, 2 serial, 3 msgType,
  // 4 lat, 5 lon, 6 landmark, 7 state, 8 country,
  // 9 distance, 10 address, 11 quality, 12 battery
  const GROUP_SIZE = 13;
  const assets = [];

  for (let i = start; i + GROUP_SIZE - 1 <= lines.length; i += GROUP_SIZE) {
    const g = lines.slice(i, i + GROUP_SIZE);

    const latitude = parseFloat(g[4]);
    const longitude = parseFloat(g[5]);

    assets.push({
      assetId: g[0],
      obsTime: g[1],
      serialNum: g[2],
      messageType: g[3],
      latitude,
      longitude,
      landmark: g[6],
      state: g[7],
      country: g[8],
      distanceFromLandmark: g[9],
      address: g[10],
      quality: g[11],
      batteryStatus: g[12],
    });
  }

  // Filter out rows that don't look like real asset data (footer/junk rows),
  // instead of assuming there's always exactly one trailing junk row.
  const validAssets = [];
  const droppedRows = [];

  for (const asset of assets) {
    const looksValid =
      asset.assetId &&
      asset.assetId.trim().length > 0 &&
      Number.isFinite(asset.latitude) &&
      Number.isFinite(asset.longitude);

    if (looksValid) {
      validAssets.push(asset);
    } else {
      droppedRows.push(asset);
    }
  }

  if (droppedRows.length > 0) {
    console.warn(
      `parseAssets: dropped ${droppedRows.length} row(s) that didn't look like valid asset data (missing assetId or non-numeric lat/lon):`,
      droppedRows.map((r) => r.assetId || "(no id)")
    );
  }

  


  return validAssets;
}