// state.js
const { Pool } = require("pg");



const pool = new Pool({
  user: process.env.PG_USER ,
  host: process.env.PG_HOST ,       
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT || 5432,
  ssl: false    // Use true if you're using SSL (not likely on local VPS)
});
const schema = "public";
const tableName = "trailer_status";
const param = `${schema}.${tableName}`;



async function connectToDatabase() {  
    let client = null;
  try{
     client = await pool.connect();  
  }
  catch(err){
    console.error('❌ Database connection Error!!! :', err);
  }  
  
  return client;
}

async function tableExists() {
  const { rows } = await pool.query(
    "SELECT to_regclass($1) IS NOT NULL AS exists",
    [param]
  );
  return rows[0]?.exists === true;
}


// Initialize the table if not exists
async function ensureStateTableExists({ strict = false } = {}) {
    if (await tableExists()) return;

     if (strict) {
        const db = process.env.PG_DATABASE || "<unknown db>";
        throw new Error(`Required table ${param} is missing in database ${db}`);
      }
    
    try{
        await pool.query(`
          Create table ${param}(
             trailer_number      text PRIMARY KEY,
              latitude            double precision,
              longitude           double precision,
              full_address        text,
              address_street      text,
              address_city        text,
              address_state       text,
              address_country     text,
              address_postal      text,
              idle_duration       text,
              serial_data         text,
              time_utc            timestamptz,              
              updated_at          timestamptz DEFAULT now()
        )`);
    }
    catch(err){
        console.error(err);
    }   
 
}


async function upsertTrailer(trailerNumber, t) {
  const query = `
    INSERT INTO ${param} (
      trailer_number,
      latitude,
      longitude,
      full_address,
      address_street,
      address_city,
      address_state,
      address_country,
      address_postal,
      idle_duration,
      serial_data,
      time_utc,
      last_updated_ago
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
    )
    ON CONFLICT (trailer_number) DO UPDATE
    SET
      latitude         = EXCLUDED.latitude,
      longitude        = EXCLUDED.longitude,
      full_address     = EXCLUDED.full_address,
      address_street   = EXCLUDED.address_street,
      address_city     = EXCLUDED.address_city,
      address_state    = EXCLUDED.address_state,
      address_country  = EXCLUDED.address_country,
      address_postal   = EXCLUDED.address_postal,
      idle_duration    = EXCLUDED.idle_duration,
      serial_data      = EXCLUDED.serial_data,
      time_utc         = EXCLUDED.time_utc,
      last_updated_ago = EXCLUDED.last_updated_ago,
      updated_at       = NOW();
  `;

  const values = [
    trailerNumber,
    t.latitude ?? null,
    t.longitude ?? null,
    t.fullAddress ?? null,
    t.addressStreet ?? null,
    t.addressCity ?? null,
    t.addressState ?? null,
    t.addressCountry ?? null,
    t.addressPostal ?? null,
    t.idleDuration ?? null,
    t.serialData ?? null,
    t.time ? new Date(t.time) : null,   // time string → timestamptz
    t.lastUpdatedAgo ?? null,
  ];

  await pool.query(query, values);
}



/**
 * Bulk upsert trailers in batches using a single INSERT ... ON CONFLICT per batch.
 * trailerNumberToTrailerInfoMap is expected to be a Map(trailerNumber -> trailerObj).
 */
async function bulkUpsertTrailerMap(trailerNumberToTrailerInfoMap, batchSize = 200) {
  // Convert Map to array of [trlNumber, trlObj]
  const entries = Array.from(trailerNumberToTrailerInfoMap.entries());
  if (entries.length === 0) {
    console.log("bulkUpsertTrailerMap: nothing to save.");
    return;
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (let offset = 0; offset < entries.length; offset += batchSize) {
      const batch = entries.slice(offset, offset + batchSize);

      const values = [];
      const valuePlaceholders = [];

      batch.forEach(([trlNumber, t], idxInBatch) => {
        // 12 columns per row now (no last_updated_ago)
        const baseIndex = idxInBatch * 12;

        valuePlaceholders.push(
          `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, ` +
          `$${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7}, $${baseIndex + 8}, ` +
          `$${baseIndex + 9}, $${baseIndex + 10}, $${baseIndex + 11}, $${baseIndex + 12})`
        );

        values.push(
          trlNumber,
          t.latitude ?? null,
          t.longitude ?? null,
          t.fullAddress ?? null,
          t.addressStreet ?? null,
          t.addressCity ?? null,
          t.addressState ?? null,
          t.addressCountry ?? null,
          t.addressPostal ?? null,
          t.idleDuration ?? null,
          t.serialData ?? null,
          t.time ? new Date(t.time) : null
        );
      });

      const query = `
        INSERT INTO ${param} (
          trailer_number,
          latitude,
          longitude,
          full_address,
          address_street,
          address_city,
          address_state,
          address_country,
          address_postal,
          idle_duration,
          serial_data,
          time_utc
        )
        VALUES
          ${valuePlaceholders.join(",\n          ")}
        ON CONFLICT (trailer_number) DO UPDATE
        SET
          latitude         = EXCLUDED.latitude,
          longitude        = EXCLUDED.longitude,
          full_address     = EXCLUDED.full_address,
          address_street   = EXCLUDED.address_street,
          address_city     = EXCLUDED.address_city,
          address_state    = EXCLUDED.address_state,
          address_country  = EXCLUDED.address_country,
          address_postal   = EXCLUDED.address_postal,
          idle_duration    = EXCLUDED.idle_duration,
          serial_data      = EXCLUDED.serial_data,
          time_utc         = EXCLUDED.time_utc,
          updated_at       = NOW();
      `;

      await client.query(query, values);
      console.log(
        `bulkUpsertTrailerMap: batch [${offset} - ${offset + batch.length - 1}] upserted (${batch.length} rows)`
      );
    }

    await client.query("COMMIT");
  } catch (err) {
    console.error("bulkUpsertTrailerMap: error, rolling back:", err);
    try {
      await client.query("ROLLBACK");
    } catch (rollbackErr) {
      console.error("Error during ROLLBACK:", rollbackErr);
    }
    throw err;
  } finally {
    client.release();
  }
}







async function saveTrailerMap(trailerNumberToTrailerInfoMap) {
  for (const [trlNumber, trlObj] of trailerNumberToTrailerInfoMap) {
    await upsertTrailer(trlNumber, trlObj);
  }
}













module.exports = {
  bulkUpsertTrailerMap,
  connectToDatabase,
  ensureStateTableExists,
  upsertTrailer,
  saveTrailerMap
};
