// skybitz-scraper.js

const axios = require("axios");
const cheerio = require("cheerio");
const { wrapper } = require("axios-cookiejar-support");
const { CookieJar } = require("tough-cookie");

// ---- 1) Axios client with cookie jar ----
const jar = new CookieJar();
const client = wrapper(
  axios.create({
    jar,
    withCredentials: true,
  })
);

// ---- 2) BASIC CONFIG (customize a few values) ----

// URL where credentials are posted (from the request that had strUserName/strPassword)
const LOGIN_URL = "https://insight.skybitz.com/CheckAccess";

// The page you come **from** when you press login (check in DevTools → Request Headers → Referer)
const LOGIN_REFERER =
  "https://insight.skybitz.com/login.jsp"; // <-- replace with real login page URL from browser

// Headers common to all requests
const BASE_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
  Origin: "https://insight.skybitz.com",
};

// LAABSearch URL (you already used this one)
const SEARCH_URL =
  "https://insight.skybitz.com/LAABSearch?event=menuSearchAssets&requestorUrl=/LAABSearch?event=menustartsearch&dispatchTo=/LocateAssets/NewAdvAssetSearchResults.jsp&map=no&optMulTerminal=AllGroups";

// Form fields from LAABSearch → Payload → View source
const SEARCH_DATA = {
  pgnav: "",
  chkSortOrderApplication: "on",
  groupName: "",
  assetIds: "",
  mtsns: "",
  mtids: "",
  optSearchType: "",
  recentNessOperator: "",
  timeperiod: "",
  hsortField1List:
    "0|Select A Field|asset_id|Asset Id|distance|Distance|obs_time|Observation Time|serial_num|MT S/N|",
  hsortOrder1List: "asc|Ascending|desc|Descending|",
  hsortField2List:
    "0|Select A Field|asset_id|Asset Id|distance|Distance|obs_time|Observation Time|serial_num|MT S/N|",
  hsortOrder2List: "asc|Ascending|desc|Descending|",
  hsortField3List:
    "0|Select A Field|asset_id|Asset Id|distance|Distance|obs_time|Observation Time|serial_num|MT S/N|",
  hsortOrder3List: "asc|Ascending|desc|Descending|",
  sortField1: "asset_id",
  sortField2: "obs_time",
  sortField3: "serial_num",
  sortOrder1: "asc",
  sortOrder2: "desc",
  sortOrder3: "asc",
  // add any extra keys from the bottom of "View source"
};

// ---- 3) Helper to encode form data ----
function toFormUrlEncoded(data) {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(data)) {
    params.append(k, v == null ? "" : String(v));
  }
  return params.toString();
}

// ---- 4) LOGIN: fresh cookies every run ----
async function login() {
  // This matches the payload you saw: strUserName, strPassword, go=GO
  const loginData = {
    strUserName: process.env.SKYBITZ_USER || "your-username",
    strPassword: process.env.SKYBITZ_PASS || "your-password",
    go: "GO",
  };

  const body = toFormUrlEncoded(loginData);

  const resp = await client.post(LOGIN_URL, body, {
    headers: {
      ...BASE_HEADERS,
      "Content-Type": "application/x-www-form-urlencoded",
      Referer: LOGIN_REFERER,
    },
    maxRedirects: 5, // follow redirects like browser
  });

  console.log("Login status:", resp.status);

  // Quick sanity check: if we still see "password" + "login", login probably failed
  // if (typeof resp.data === "string" && 
  //     /password/i.test(resp.data) &&
  //     /login/i.test(resp.data)) {
  //   console.warn("Warning: login response still looks like a login page.");
  // }
  // The cookie jar (jar) now holds JSESSIONID, idke, udke, etc.
}

// ---- 5) Fetch LAABSearch HTML using cookies from jar ----
async function fetchSearchHtml() {
  const body = toFormUrlEncoded(SEARCH_DATA);

  const resp = await client.post(SEARCH_URL, body, {
    headers: {
      ...BASE_HEADERS,
      "Content-Type": "application/x-www-form-urlencoded",
      Referer:
        "https://insight.skybitz.com/LAABSearch?event=menustartsearch&dispatchTo=/LocateAssets/AssetBasedSearchAssetsMultiple.jsp",
    },
    maxRedirects: 5,
  });

  const html = resp.data;

  if (typeof html === "string" &&
      /password/i.test(html) &&
      /login/i.test(html)) {
    throw new Error("LAABSearch returned login page – login probably failed.");
  }

  return html;
}

// ---- 6) Parse table (adjust indices to match your columns) ----
function parseAssets(html) {
  const $ = cheerio.load(html);

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

    // Stop when it no longer looks like an id (7+ digits)
    if (!/^\d{6,8}$/.test(g[0])) break;

    const asset = {
      assetId: g[0],
      obsTime: g[1],
      serialNum: g[2],
      messageType: g[3],
      latitude: parseFloat(g[4]),
      longitude: parseFloat(g[5]),
      landmark: g[6],
      state: g[7],
      country: g[8],
      distanceFromLandmark: g[9],
      address: g[10],
      quality: g[11],
      batteryStatus: g[12],
    };

    assets.push(asset);
  }

  return assets;
}



// ---- 7) Public function / test runner ----
async function fetchAssets() {
  await login();                // 1) fresh cookies
  const html = await fetchSearchHtml();  // 2) use them
  return parseAssets(html);     // 3) parse table
}



async function main() {
  try {
    const assets = await fetchAssets();
    console.log(`Fetched ${assets.length} assets`);    


  } catch (err) {
    console.error("ERROR:", err.message);
    if (err.response) {
      console.error("Status:", err.response.status, err.response.statusText);
    }
  }
}

if (require.main === module) {
  main();
} else {
  module.exports = { fetchAssets };
}
