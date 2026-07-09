// controllers/sync.controller.js
// Orchestrates the full sync: fetch from all three sources,
// merge into a Map, write to Postgres and Monday.com.

import { fetchSkybitzPositions } from '../services/skybitz-xml.service.js';
import { fetchSkybitzAssets }   from '../services/skybitz-scraper/index.js';
import { sendErrorToTelegram ,  sendMessageToTelegram}   from '../services/telegram.service.js';
import { bulkUpsertTrailerMap }  from '../db/trailer.repository.js';
import { fetchOrbcommAssets } from '../services/orbcomm/index.js';
import {
  makeMondayApiRequest,
  buildAliasedMutation,
  chunk, runBatches,
  BATCH_SIZE,
} from '../services/monday.service.js';
import { calculateIdleDurations, formatIdleDuration } from '../services/idle-tracker.service.js';
import {
  getAllRowIDs,
  updateMultipleAliasColumnValuesQuery,
  createMultipleAliasColumnValuesQuery,
} from '../queries/monday.queries.js';

import { formatToEasternTime, diffToText } from '../utils/time.utils.js';
import { TRAILER_BOARD_ID, TRAILER_BOARD_GROUP_ID } from '../config.js';

const EXCEPTIONS_SN = [];

function makeTrailerObj({
  latitude=null, longitude=null, fullAddress=null,
  addressStreet=null, addressCity=null, addressState=null,
  addressCountry=null, addressPostal=null, idleDuration=0,
  serialData=null, time=null, rowId=null,
} = {}) {
  return { latitude, longitude, fullAddress, addressStreet, addressCity,
    addressState, addressCountry, addressPostal, idleDuration, serialData, time, rowId  };
}

export async function runSync() {
  // ---- 1) Fetch all three sources independently ----
  const [skybitzAPIAssets, scrapSkybitzAssets , orbcommAPIAssets ] = await Promise.allSettled([
    fetchSkybitzPositions(),    
    fetchSkybitzAssets(),
    fetchOrbcommAssets()
  ]);

  if (skybitzAPIAssets.status === 'rejected')
    await sendErrorToTelegram(`⚠️ SkyBitz Bowman failed: ${skybitzAPIAssets.reason?.message}`);  
  if (scrapSkybitzAssets.status === 'rejected')
    await sendErrorToTelegram(`⚠️ SkyBitz Metro failed: ${scrapSkybitzAssets.reason?.message}`);
  if (orbcommAPIAssets.status === 'rejected')
    await sendErrorToTelegram(`⚠️ Orbcomm API failed: ${orbcommAPIAssets.reason?.message}`);

  const xmlTrailers     = skybitzAPIAssets.status === 'fulfilled' ? skybitzAPIAssets.value?.skybitz?.gls ?? [] : [];
  const scrapedAssets   = scrapSkybitzAssets.status   === 'fulfilled' ? scrapSkybitzAssets.value   : [];
  const orbcommAssets   = orbcommAPIAssets.status === 'fulfilled' ? orbcommAPIAssets.value : [];

  if (!xmlTrailers.length && !orbcommAssets.length && !scrapedAssets.length) {
    await sendErrorToTelegram('❌ All three data sources failed — aborting sync.');
    return;
  }

  // ---- 2) Merge all sources into one Map ----
  const trailerMap = new Map();

  for (const trl of xmlTrailers) {
    if (EXCEPTIONS_SN.includes(trl?.mtsn)) continue;
    const street = trl?.address?.street, city = trl?.address?.city;
    const state  = trl?.address?.state,  postal = trl?.address?.postal;
    const fullAddress =
      (street ? `${street},` : '') + (city   ? ` ${city},`   : '') +
      (state  ? ` ${state},` : '') + (postal ? ` ${postal}`  : '');

    trailerMap.set(String(trl?.asset?.assetid), makeTrailerObj({
      latitude: trl?.latitude, 
      longitude: trl?.longitude, 
      fullAddress,
      addressStreet: street, 
      addressCity: city, 
      addressState: state,
      addressCountry: trl?.address?.country, 
      addressPostal: postal,      
      serialData: trl?.serial?.serialdata,
      time: formatToEasternTime(new Date(trl?.['time-iso8601'])),
    }));
  }
 

  for (const asset of scrapedAssets) {
    const fullAddress = asset.address !== 'n/a' ? asset.address : `${asset.landmark}, ${asset.state}`;
    trailerMap.set(asset.assetId, makeTrailerObj({
      latitude: asset.latitude, 
      longitude: asset.longitude,
      fullAddress, 
      addressState: asset.state, 
      time: asset.obsTime + ' EST',
    }));
  }

  for (const asset of orbcommAssets) {
      const fullAddress = asset.address !== null ? asset.address : `${asset.city}, ${asset.state} ${asset.zipCode}`;
      trailerMap.set(asset.assetId, makeTrailerObj({
        latitude: asset.latitude, 
        longitude: asset.longitude,
        fullAddress, 
        addressState: asset.state, 
        addressStreet: asset.street,
        addressCountry : asset.country,
        addressPostal : asset.zipCode,
        serialData : asset.moving,
        time: formatToEasternTime(asset.obsTime),
      }));
  }





 
  // ---- 3) Fetch Monday.com row IDs ----
try{


    // ---- 4) Calculate idle durations ----
    // Must happen BEFORE saving to DB so we read previous positions first
    await calculateIdleDurations(trailerMap);


    const mondayResponse = await makeMondayApiRequest(getAllRowIDs(TRAILER_BOARD_ID));


    const itemsArray = mondayResponse?.data?.boards[0]?.items_page?.items;

    if (!Array.isArray(itemsArray) || !itemsArray.length) {
      await sendErrorToTelegram('Empty Monday.com response — aborting sync.');
      return;
    }

    for (const item of itemsArray) {
      const trlObj = trailerMap.get(String(item?.name));
      if (trlObj) trlObj.rowId = item?.id;
    }

    // ---- 5) Build Monday.com ops ----
    const ops = [];
    for (const [trlNumber, trlObj] of trailerMap) {
      const colValues = {
        'link_mktvvmv': {
          url:  `https://www.google.com/maps/search/?api=1&query=${trlObj.latitude},${trlObj.longitude}`,
          text: `${trlObj.fullAddress}`,
        },
        'text_mktvv1mz': `${trlObj.addressState}`,
        'text_mkxnr5nm': `${trlObj.serialData}`,
        'text_mkxnv1fc': `${trlObj.time}`,
        'text_mky8qezb': `${diffToText(trlObj.time)}`,
        'text_mm53p4px':     `${formatIdleDuration(trlObj.idleDuration)}`,
      };

      if (trlObj.rowId)
        ops.push(updateMultipleAliasColumnValuesQuery(TRAILER_BOARD_ID, trlObj.rowId, colValues));
      else
        ops.push(createMultipleAliasColumnValuesQuery(ops.length, TRAILER_BOARD_ID, TRAILER_BOARD_GROUP_ID, trlNumber, colValues));
    }

    // ---- 6) Save to Postgres + sync Monday.com ----
    await bulkUpsertTrailerMap(trailerMap);
    const batches = chunk(ops, BATCH_SIZE).map(buildAliasedMutation);
    if (batches.length) await runBatches(batches);

    await sendMessageToTelegram(`✅ Sync complete — ${trailerMap.size} trailers processed`);

  }catch(err){
      await sendErrorToTelegram(`Error processing the data : ${err.message}`);
  }
  
}

