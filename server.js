require('dotenv').config();

const fetch = require('node-fetch');
const express = require('express');
const { google } = require('googleapis');
const { XMLParser } = require('fast-xml-parser');
const cron = require("node-cron");

const dbQueries = require('./db-queries');
const mondayQueries = require('./monday-queries');
const scrapper = require('./Scrapper.js');


const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID = process.env.TELEGRAM_USER_ID;

const SKYBITZ_BASE = process.env.SKYBITZ_BASE || 'https://xml.skybitz.com:9443';
const SKYBITZ_CUSTOMER = process.env.SKYBITZ_CUSTOMER;
const SKYBITZ_PASSWORD = process.env.SKYBITZ_PASSWORD;
const SKYBITZ_VERSION = process.env.SKYBITZ_VERSION || '2.74';

const MONDAY_API_TOKEN = process.env.MONDAY_API_TOKEN;
const MONDAY_BASE_URL = process.env.MONDAY_BASE_URL;
const TRAILER_BOARD_ID = process.env.TRAILER_BOARD_ID;
const TRAILER_BOARD_GROUP_ID = process.env.TRAILER_BOARD_GROUP_ID;

const SAMSARA_API = process.env.SAMSARA_API_TOKEN;
const SAMSARA_BASE_URL = process.env.SAMSARA_BASE_URL; // trailer url 



const SLEEP = (ms) => new Promise(r => setTimeout(r, ms));
const MAX_RETRIES = 4;
const BASE_BACKOFF_MS = 600;
const BATCH_SIZE = 20;        // << group into 20 ops
const BATCH_CONCURRENCY = 3;  // run up to 3 batches in parallel (tweak if needed)



const app = express();
app.use(express.json());
let requestBody;

app.get( '/health' , async (req, res) => {
     res.status(200).json({ status: 'ok' });
});

const capAfterSpaces = s =>
  s.replace(/^\s*\p{L}/u, c => c.toUpperCase());



app.get( '/' , async (req, res) => {

  res.status(200).json({ ok: true });   
    
});



async function main() {
  
  try{

      
   
    const [skybitzPositions, samsaraTrailerResponse, assets] = await Promise.all([
          fetchSkybitzPositions(),
          makeApiRequest(SAMSARA_BASE_URL, 'GET', undefined, SAMSARA_API),
          scrapper.fetchAssets(),
        ]);
    

   


    const allTrailerDataArray = skybitzPositions.skybitz.gls;

    const samsaraTrailersInfoArray = samsaraTrailerResponse?.assets;
    

    const trailerNumberToTrailerInfoMap = new Map();

    const exceptionsMTSN = ["SHB7BJBL231902665"];

    for(let trailerInfo of allTrailerDataArray){
      const trailerNumber = String(trailerInfo?.asset?.assetid);
      const latitude = trailerInfo?.latitude;
      const longitude = trailerInfo?.longitude;

      const landmarkGeoname = trailerInfo?.landmark?.geoname;
      const landmarkCity = trailerInfo?.landmark?.city;
      const landmarkState = trailerInfo?.landmark?.state;
      const landmarkCountry = trailerInfo?.landmark?.country;
      const landmarkPostal = trailerInfo?.landmark?.postal;
      const landmarkDistance = trailerInfo?.landmark?.distance;
      const landmarkDirection = trailerInfo?.landmark?.direction;

      const addressStreet = trailerInfo?.address?.street;
      const addressCity = trailerInfo?.address?.city;
      const addressState = trailerInfo?.address?.state;
      const addressCountry = trailerInfo?.address?.country;
      const addressPostal = trailerInfo?.address?.postal;

      const idleDuration = trailerInfo?.idle?.idleduration;

      const serialData = trailerInfo?.serial?.serialdata;

      const trailerMTSN = trailerInfo?.mtsn;

      const time = formatToEasternTime(new Date(trailerInfo?.["time-iso8601"]));

      
      

       const fullAddress = 
              (addressStreet ? `${addressStreet},` : '') +
              (addressCity ? ` ${addressCity},` : '') +
              (addressState ? ` ${addressState},` : '') +
              (addressPostal ? ` ${addressPostal}` : '');


      const trlobj = makeTrailerObj({        
        latitude,
        longitude,
        fullAddress,
        addressStreet,
        addressCity,
        addressState,
        addressCountry,
        addressPostal,
        idleDuration,
        serialData,
        time,        
        rowId : null
      });

      




      if(!exceptionsMTSN.includes(trailerMTSN)){
        trailerNumberToTrailerInfoMap.set(trailerNumber,trlobj); 
      }
      

    }

    for(let trailerInfo of samsaraTrailersInfoArray){

      const trailerNumber = String(trailerInfo?.name).replace(/^TRL#\s*/, '');
      
      const latitude = trailerInfo?.location?.[0]?.latitude;
      const longitude = trailerInfo?.location?.[0]?.longitude;
      const time = formatToEasternTime(new Date(trailerInfo?.location?.[0]?.timeMs));
      const fullAddress = trailerInfo?.location?.[0]?.location;
      const addressState = extractState(fullAddress);
      const addressStreet = null;
      const addressCity = null;
      const addressCountry = null;
      const addressPostal =null;
      const idleDuration = null;
      const serialData = null;
      

      const trlobj = makeTrailerObj({        
        latitude,
        longitude,
        fullAddress,
        addressStreet,
        addressCity,
        addressState,
        addressCountry,
        addressPostal,
        idleDuration,
        serialData,        
        time,
        rowId : null
      });

       trailerNumberToTrailerInfoMap.set(trailerNumber,trlobj); 

    }

    

    for(let asset of assets){
        
      const trailerNumber = asset.assetId;
      
      const latitude = asset.latitude;
      const longitude = asset.longitude;
      const time = asset.obsTime + " EST";
      const fullAddress = asset.address !='n/a' ? asset.address : asset.landmark+", "+asset.state;
      const addressState = asset.state;
      const addressStreet = null;
      const addressCity = null;
      const addressCountry = null;
      const addressPostal =null;
      const idleDuration = null;
      const serialData = null;
      

      const trlobj = makeTrailerObj({        
        latitude,
        longitude,
        fullAddress,
        addressStreet,
        addressCity,
        addressState,
        addressCountry,
        addressPostal,
        idleDuration,        
        serialData,
        time,
        rowId : null
      });

      trailerNumberToTrailerInfoMap.set(trailerNumber,trlobj);



    }



   

    const mondayResponse = await makeApiRequest(MONDAY_BASE_URL,'POST',mondayQueries.getAllRowIDs(TRAILER_BOARD_ID), MONDAY_API_TOKEN);

    const itemsArray = mondayResponse?.data?.boards[0]?.items_page?.items;
    
   

    for(const item of itemsArray){
      
      const itemName = String(item?.name);
      const trlObj = trailerNumberToTrailerInfoMap.get(itemName);

       if (!trlObj) {          
          continue;
       }  

      trlObj.rowId = item?.id;
    }


    


    let ops = [];
   
    for (const [trlNumber, trlObj] of trailerNumberToTrailerInfoMap) {
      
           
            const lastUpdatedAgo = diffToText(trlObj.time);


            const colValues ={
              "link_mktvvmv":{                   // change link column 
                "url":`www.google.com/maps/search/?api=1&query=${trlObj.latitude},${trlObj.longitude}`,
                "text" :`${trlObj.fullAddress}`
              },
              "text_mktvv1mz":`${trlObj.addressState}`,          //state column
              "text_mkxnr5nm":`${trlObj.serialData}`,
              "text_mkxnv1fc":`${trlObj.time}`,
              "text_mky8qezb":`${lastUpdatedAgo}`
            }

            if(trlObj.rowId){        
                
              ops.push(mondayQueries.updateMultipleAlliasColumnValuesQuery(TRAILER_BOARD_ID,trlObj.rowId,colValues));                
                
            }else{
              ops.push(mondayQueries.createMultipleAlliasColumnValuesQuery(ops.length,TRAILER_BOARD_ID,TRAILER_BOARD_GROUP_ID,trlNumber,colValues));              
            }

           
    }
   
 

    const updateBatches = chunk(ops, BATCH_SIZE).map(buildAliasedMutation);
    
    await dbQueries.bulkUpsertTrailerMap(trailerNumberToTrailerInfoMap);


    if (updateBatches.length) {
      await runBatches(updateBatches);
    }

    

    


    
          

  }catch(error){
    console.error("❌ Error fetching Info:", error);   
    await sendErrorToTelegram(`❌ Error occurred: ${error.message || error}`);
    console.log(requestBody);
  }

}


function makeTrailerObj({
  latitude,
  longitude,
  fullAddress,
  addressStreet = null,
  addressCity = null,
  addressState = null,
  addressCountry = null,
  addressPostal = null,
  idleDuration = null,
  serialData = null,
  time = null,  
  rowId = null,
}) {
  return {
    latitude,
    longitude,
    fullAddress,
    addressStreet,
    addressCity,
    addressState,
    addressCountry,
    addressPostal,
    idleDuration,
    serialData,
    time, 
    rowId,
  };
}








function extractState(address) {
  if (!address) return null;
  
  const stateRegex = /(?:^|,\s*)(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)(?=,|$|\s\d{5})/;

  const match = address.match(stateRegex);
  return match ? match[1] : null;   // return just the state code, e.g. "VA"
}






(async () => {
  try {    
    await dbQueries.ensureStateTableExists({ strict: true });
    const PORT = process.env.PORT || 3003;
    app.listen(PORT, () => console.log(`Server is running on PORT ${PORT}`));
  } catch (e) {
    console.error('DB init failed', e);
    try {
      await sendErrorToTelegram(`DB init failed: ${e.message || e}`);
    } catch (_) {}
    process.exit(1);
  }
})();



async function sendErrorToTelegram(messageText) {
  const message = `🚨 *Alert!* 🚨\n\n${escapeMarkdown(messageText)}`;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  const params = {
    chat_id: CHAT_ID,
    text: message,
    parse_mode: "Markdown"
  };

  try {
    const telegramRes = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params)
    });

    if (!telegramRes.ok) {
      const errorText = await telegramRes.text();
      throw new Error(`Telegram API error: ${errorText}`);
    }

    console.log("✅ Telegram alert sent");
  } catch (err) {
    console.error("❌ Failed to send Telegram message:", err);
  }
}









function formatToEasternTime(input) {
  const date = new Date(input);

  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    month: 'short',       // "May"
    day: 'numeric',      // "2"
    year: 'numeric',     // "2025"
    hour: 'numeric',     // "2"
    minute: '2-digit',   // "07"
    hour12: true,        // "PM"
    timeZoneName: 'short' // "EDT"
  }).format(date);
}


function diffToText(earlier, later = new Date()) {
  // Accept Date objects or anything Date can parse (string, timestamp)
  const start = (earlier instanceof Date) ? earlier : new Date(earlier);
  const end   = (later   instanceof Date) ? later   : new Date(later);

  let diffMs = end - start;

  if (diffMs < 0) {
    // if earlier is actually in the future
    diffMs = -diffMs;
  }

  const totalMinutes = Math.floor(diffMs / (1000 * 60));
  const days   = Math.floor(totalMinutes / (60 * 24));
  const hours  = Math.floor((totalMinutes % (60 * 24)) / 60);
  const mins   = totalMinutes % 60;

  const parts = [];
  if (days)  parts.push(`${days} day${days !== 1 ? "s" : ""}`);
  if (hours) parts.push(`${hours} hour${hours !== 1 ? "s" : ""}`);
  if (mins || parts.length === 0) {
    parts.push(`${mins} minute${mins !== 1 ? "s" : ""}`);
  }

  return parts.join(" ") + " ago";
}





function escapeMarkdown(t) {
  return String(t).replace(/([_*[\]()`])/g, '\\$1');  // legacy Markdown
}


function kphToMph(kph) {
  if (typeof kph !== "number" || !Number.isFinite(kph)) {
    throw new TypeError("kph must be a finite number");
  }
  const KM_TO_MILES = 0.621371; // 1 km ≈ 0.621371 miles
  return kph * KM_TO_MILES;
}


function kphToMphRounded(kph, decimals = 2) {
  const mph = kphToMph(kph);
  const p = 10 ** decimals;
  return Math.round(mph * p) / p;
}









function getAuth() {
  
  const privateKey = process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n');
  return new google.auth.JWT({
    email: process.env.GOOGLE_CLIENT_EMAIL,
    key: privateKey,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
}



 async function appendRows(rows) {
  // rows: array of arrays, e.g. [[timestamp, alertType, driver, vehicle, severity, speed]]
  const auth = getAuth();
  const sheets = google.sheets({ version: 'v4', auth });

  const range = `${process.env.SHEET_NAME}!A:Z`;
  await sheets.spreadsheets.values.append({
    spreadsheetId: process.env.SPREADSHEET_ID,
    range,
    valueInputOption: 'USER_ENTERED',    // lets you use dates/formulas naturally
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: rows },
  });
}



async function logAlertToSheet( alertType,driverName,vehicleNumber,formattedDate,severity,speedRange,forwardVideoUrl,inwardVideoUrl ) {
  const ts = formatToEasternTime(new Date().toISOString());
  await appendRows([[alertType, driverName, vehicleNumber, formattedDate, severity ,speedRange ,forwardVideoUrl,inwardVideoUrl,ts]]);
}



async function makeApiRequest(baseURL,method,query,API_TOKEN) {
    try {
        const options = {
          method,
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${API_TOKEN}`,
          },
        };

         if (query !== undefined && query !== null && method !== 'GET') {
            options.body = JSON.stringify({ query });
          }

          const response = await fetch(baseURL, options);

        if (!response.ok) {
            throw new Error(`API request failed: ${response.statusText}`);
        }

        return await response.json();
    } catch (error) {
        console.error('Error in API request:', error);
        return null;
    }
}



async function fetchSkybitzPositions() {
  const url = `${SKYBITZ_BASE}/QueryPositions?assetid=ALL` +
    `&customer=${encodeURIComponent(SKYBITZ_CUSTOMER)}` +
    `&password=${encodeURIComponent(SKYBITZ_PASSWORD)}` +
    `&version=${encodeURIComponent(SKYBITZ_VERSION)}`;

  const r = await fetch(url, { method: 'GET' });
  if (!r.ok) throw new Error(`SkyBitz HTTP ${r.status} ${await r.text()}`);
  const xml = await r.text();
  const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '' });
  return parser.parse(xml); // returns a JS object
}





function vincentyDistance(lat1, lon1, lat2, lon2) {
  const toRad = d => (d * Math.PI) / 180;

  // WGS-84 parameters
  const a = 6378137.0;
  const f = 1 / 298.257223563;
  const b = a * (1 - f);

  const phi1 = toRad(lat1);
  const phi2 = toRad(lat2);
  const L = toRad(lon2 - lon1);

  const U1 = Math.atan((1 - f) * Math.tan(phi1));
  const U2 = Math.atan((1 - f) * Math.tan(phi2));
  const sinU1 = Math.sin(U1), cosU1 = Math.cos(U1);
  const sinU2 = Math.sin(U2), cosU2 = Math.cos(U2);

  let lambda = L;
  let lambdaPrev;
  const maxIter = 200;
  const tol = 1e-12;

  let sinSigma, cosSigma, sigma, sinAlpha, cos2Alpha, cos2SigmaM;

  for (let i = 0; i < maxIter; i++) {
    const sinLambda = Math.sin(lambda), cosLambda = Math.cos(lambda);

    sinSigma = Math.sqrt(
      (cosU2 * sinLambda) ** 2 +
      (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda) ** 2
    );
    if (sinSigma === 0) return 0; // coincident points

    cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda;
    sigma = Math.atan2(sinSigma, cosSigma);
    sinAlpha = (cosU1 * cosU2 * sinLambda) / sinSigma;
    cos2Alpha = 1 - sinAlpha ** 2;

    // Equatorial line: cos2Alpha = 0 → cos2SigmaM = 0
    cos2SigmaM = (cos2Alpha !== 0)
      ? (cosSigma - (2 * sinU1 * sinU2) / cos2Alpha)
      : 0;

    const C = (f / 16) * cos2Alpha * (4 + f * (4 - 3 * cos2Alpha));
    lambdaPrev = lambda;
    lambda = L + (1 - C) * f * sinAlpha *
      (sigma + C * sinSigma * (cos2SigmaM + C * cosSigma * (-1 + 2 * cos2SigmaM ** 2)));

    if (Math.abs(lambda - lambdaPrev) < tol) break; // converged
    if (i === maxIter - 1) return NaN;               // failed to converge
  }

  const uSquared = cos2Alpha * ((a * a - b * b) / (b * b));
  const A = 1 + (uSquared / 16384) * (4096 + uSquared * (-768 + uSquared * (320 - 175 * uSquared)));
  const B = (uSquared / 1024) * (256 + uSquared * (-128 + uSquared * (74 - 47 * uSquared)));

  const deltaSigma = B * sinSigma * (cos2SigmaM + (B / 4) *
    (cosSigma * (-1 + 2 * cos2SigmaM ** 2) -
     (B / 6) * cos2SigmaM * (-3 + 4 * sinSigma ** 2) * (-3 + 4 * cos2SigmaM ** 2)));

  const s = b * A * (sigma - deltaSigma);
  return s; // meters
}




function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}


function escapeGraphQLString(s = "") {
  return String(s)
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r');
}

/** Build one GraphQL mutation with many aliased ops. `ops` is array of strings like:
 *  `op1: move_item_to_group(item_id: 123, group_id: "group_x"){ id }`
 */
function buildAliasedMutation(ops) {
  return `mutation{\n${ops.join('\n')}\n}`;
}

async function makeMondayApiRequest(query) {
  let attempt = 0;
  while (true) {
    try {
      const response = await fetch('https://api.monday.com/v2', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${MONDAY_API_TOKEN}`,
        },
        body: JSON.stringify({ query }),
      });

      const json = await response.json().catch(() => ({}));

      if (!response.ok) {
        const retryable = [429, 502, 503, 504].includes(response.status);
        if (retryable && attempt < MAX_RETRIES) {
          await SLEEP(BASE_BACKOFF_MS * Math.pow(2, attempt++));
          continue;
        }
        throw new Error(`HTTP ${response.status} ${response.statusText}: ${JSON.stringify(json)}`);
      }

      if (json.errors && json.errors.length) {
        const msg = JSON.stringify(json.errors);
        const transient = /rate|complexity|timeout|temporary/i.test(msg);
        if (transient && attempt < MAX_RETRIES) {
          await SLEEP(BASE_BACKOFF_MS * Math.pow(2, attempt++));
          continue;
        }
        throw new Error(`GraphQL errors: ${msg}`);
      }

      return json;
    } catch (err) {
      if (attempt < MAX_RETRIES) {
        await SLEEP(BASE_BACKOFF_MS * Math.pow(2, attempt++));
        continue;
      }
      throw err;
    }
  }
}

/** Run many batch strings with limited parallelism */
async function runBatches(batchQueries, concurrency = BATCH_CONCURRENCY) {
  let i = 0;
  const workers = new Array(Math.min(concurrency, batchQueries.length)).fill(0).map(async () => {
    while (i < batchQueries.length) {
      const idx = i++;
      await makeMondayApiRequest(batchQueries[idx]);
    }
  });
  await Promise.all(workers);
}





let isRunning = false; // to prevent overlap


cron.schedule(
  "*/30 * * * *",  // every 30 minutes
  async () => {
    if (isRunning) {
      console.log("Cron: previous run still in progress, skipping this one");
      return;
    }

    isRunning = true;
    console.log("Cron: starting runSkybitzUpdate at", new Date().toISOString());

    try {
      await main();
      console.log("Cron: finished runSkybitzUpdate");
    } catch (err) {
      console.error("Cron: error in runSkybitzUpdate:", err);
    } finally {
      isRunning = false;
    }
  },
  {
    timezone: "America/New_York", // optional, but nice for your EST use case
  }
);