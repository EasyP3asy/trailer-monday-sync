// skybitz.js
//
// The one file server.js will import. Ties the other pieces together in
// order: login -> fetch html -> parse html -> return assets.


import { login } from "./auth.js";
import { fetchSearchHtml } from "./fetcher.js";
import { parseAssets } from "./parser.js";
import { pathToFileURL } from "url";


export async function fetchSkybitzAssets() {
  await login(); // 1) fresh cookies  
  const html = await fetchSearchHtml(); // 2) use them
  return parseAssets(html); // 3) parse table
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



if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  main();
}