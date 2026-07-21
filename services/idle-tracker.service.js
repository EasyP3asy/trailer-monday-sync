// services/idle-tracker.service.js
//
// Calculates idle time for each trailer by comparing its current
// position (from the API fetch) against its previous position (stored
// in the DB from the last run).
//
// Logic:
//   distance < 1.5 miles → didn't move → add 30 minutes to idle_duration
//   distance >= 1.5 miles → moved      → reset idle_duration to 0

import { pool } from '../db/pool.js';
import { vincentyDistance } from '../utils/geo.utils.js';

const MILES_THRESHOLD  = 1.5;
const METERS_PER_MILE  = 1609.344;
const IDLE_INCREMENT   = 30; // minutes added per run when not moving

// ---- Fetch all previous positions from DB ----
async function fetchPreviousPositions() {
  const { rows } = await pool.query(`
    SELECT trailer_number, latitude, longitude, idle_duration
    FROM public.trailer_status
  `);

  // Convert to a Map for O(1) lookup by trailer number
  const map = new Map();
  for (const row of rows) {
    map.set(row.trailer_number, {
      latitude:      row.latitude,
      longitude:     row.longitude,
      idleDuration:  row.idle_duration ?? 0,
    });
  }
  return map;
}

// ---- Calculate idle duration for each trailer ----
// trailerMap: Map(trailerNumber → trailerObj) — current run's data
// Returns the same map with idleDuration updated on each trailerObj
export async function calculateIdleDurations(trailerMap) {
  const previousPositions = await fetchPreviousPositions();

  for (const [trailerNumber, trailerObj] of trailerMap) {
    const prev = previousPositions.get(trailerNumber);

    // No previous position in DB yet — first time we've seen this trailer
    if (!prev || prev.latitude == null || prev.longitude == null) {
      trailerObj.idleDuration = 0;
      continue;
    }

    // No current position — can't calculate distance
    if (trailerObj.latitude == null || trailerObj.longitude == null) {
      trailerObj.idleDuration = prev.idleDuration;
      continue;
    }

    // Calculate distance between previous and current position
    const distanceMeters = vincentyDistance(
      prev.latitude,
      prev.longitude,
      trailerObj.latitude,
      trailerObj.longitude
    );

    const distanceMiles = distanceMeters / METERS_PER_MILE;

    if (distanceMiles < MILES_THRESHOLD) {
      // Trailer hasn't moved — add 30 minutes to existing idle time
      trailerObj.idleDuration = prev.idleDuration + IDLE_INCREMENT;
      
    } else {
      // Trailer moved — reset idle time
      trailerObj.idleDuration = 0;      
    }
  }

  return trailerMap;
}

// ---- Format minutes into human readable string ----
// Used for display in Monday.com column
export function formatIdleDuration(minutes) {
  if (!minutes || minutes === 0) return 'Moving';

  const days  = Math.floor(minutes / (60 * 24));
  const hours = Math.floor((minutes % (60 * 24)) / 60);
  const mins  = minutes % 60;

  const parts = [];
  if (days)  parts.push(`${days}d`);
  if (hours) parts.push(`${hours}h`);
  if (mins)  parts.push(`${mins}m`);

  return parts.join(' ');
}