// db/trailer.repository.js
// All DB operations for the trailer_status table.

import { pool } from './pool.js';
import { PG_DATABASE } from '../config.js';

const param = 'public.trailer_status';

async function tableExists() {
  const { rows } = await pool.query('SELECT to_regclass($1) IS NOT NULL AS exists', [param]);
  return rows[0]?.exists === true;
}

export async function ensureTableExists({ strict = false } = {}) {
  if (await tableExists()) return;
  if (strict) throw new Error(`Required table ${param} is missing in database ${PG_DATABASE}`);

  await pool.query(`
    CREATE TABLE ${param} (
      trailer_number  text PRIMARY KEY,
      latitude        double precision,
      longitude       double precision,
      full_address    text,
      address_street  text,
      address_city    text,
      address_state   text,
      address_country text,
      address_postal  text,
      idle_duration   integer DEFAULT 0,
      serial_data     text,
      time_utc        timestamptz,
      updated_at      timestamptz DEFAULT now()
    )
  `);
}

export async function bulkUpsertTrailerMap(trailerMap, batchSize = 200) {
  const entries = Array.from(trailerMap.entries());
  if (!entries.length) { console.log('DB: nothing to save.'); return; }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (let offset = 0; offset < entries.length; offset += batchSize) {
      const batch = entries.slice(offset, offset + batchSize);
      const values = [], placeholders = [];

      batch.forEach(([trlNumber, t], idx) => {
        const b = idx * 12;
        placeholders.push(`($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},$${b+7},$${b+8},$${b+9},$${b+10},$${b+11},$${b+12})`);
        values.push(
          trlNumber,
          t.latitude        ?? null,
          t.longitude       ?? null,
          t.fullAddress     ?? null,
          t.addressStreet   ?? null,
          t.addressCity     ?? null,
          t.addressState    ?? null,
          t.addressCountry  ?? null,
          t.addressPostal   ?? null,
          t.idleDuration    ?? 0,      // integer, default 0 not null
          t.serialData      ?? null,
          t.time ? new Date(t.time) : null
        );
      });

      await client.query(`
        INSERT INTO ${param} (trailer_number,latitude,longitude,full_address,address_street,
          address_city,address_state,address_country,address_postal,idle_duration,serial_data,time_utc)
        VALUES ${placeholders.join(',\n')}
        ON CONFLICT (trailer_number) DO UPDATE SET
          latitude        = EXCLUDED.latitude,
          longitude       = EXCLUDED.longitude,
          full_address    = EXCLUDED.full_address,
          address_street  = EXCLUDED.address_street,
          address_city    = EXCLUDED.address_city,
          address_state   = EXCLUDED.address_state,
          address_country = EXCLUDED.address_country,
          address_postal  = EXCLUDED.address_postal,
          idle_duration   = EXCLUDED.idle_duration,
          serial_data     = EXCLUDED.serial_data,
          time_utc        = EXCLUDED.time_utc,
          updated_at      = NOW();
      `, values);

      console.log(`DB: batch [${offset}-${offset+batch.length-1}] upserted (${batch.length} rows)`);
    }

    await client.query('COMMIT');
  } catch (err) {
    console.error('DB: error, rolling back:', err);
    try { await client.query('ROLLBACK'); } catch(e) { console.error('ROLLBACK error:', e); }
    throw err;
  } finally {
    client.release();
  }
}