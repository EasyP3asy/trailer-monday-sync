import 'dotenv/config';
// skybitz-config.js
//
// All the "data" of the scraper lives here: URLs, headers, and the
// SEARCH_DATA form payload copied from DevTools → Network → Payload.
// Nothing in this file makes a request or touches the DOM — it's just
// constants, so it's safe to import from anywhere without side effects.

// URL where credentials are posted (from the request that had strUserName/strPassword)
export const LOGIN_URL = "https://insight.skybitz.com/CheckAccess";

export const ACTIVATE_URL = "https://insight.skybitz.com/CheckAccess?redirect=legacy";

// The page you come **from** when you press login (check in DevTools → Request Headers → Referer)
export const LOGIN_REFERER = "https://insight.skybitz.com/login.jsp"; // <-- replace with real login page URL from browser

export const LOGIN_REFERER_ACTIVATION = "https://insight.skybitz.com/ng/dashboards/operations";
// Headers common to all requests
export const BASE_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
  Origin: "https://insight.skybitz.com",
};

// LAABSearch URL (you already used this one)
export const SEARCH_URL =
  "https://insight.skybitz.com/LAABSearch?event=menuSearchAssets&requestorUrl=/LAABSearch?event=menustartsearch&dispatchTo=/LocateAssets/NewAdvAssetSearchResults.jsp&map=no&optMulTerminal=AllGroups";

// Referer specifically for the search request (different page than login)
export const SEARCH_REFERER =
  "https://insight.skybitz.com/LAABSearch?event=menustartsearch&dispatchTo=/LocateAssets/AssetBasedSearchAssetsMultiple.jsp";

// Form fields from LAABSearch → Payload → View source
export const SEARCH_DATA = {
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

// Credentials come from env vars at call time, not hardcoded here —
// see skybitz-auth.js
export const SKYBITZ_USER = process.env.SKYBITZ_USER;
export const SKYBITZ_PASS = process.env.SKYBITZ_PASS;