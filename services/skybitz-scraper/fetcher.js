// skybitz-fetcher.js
//
// One job: POST the search form and return the raw HTML. Relies on
// skybitz-auth.js having already called login() so the shared cookie jar
// has valid session cookies — this file doesn't log in itself.

import { client } from "./http-client.js";
import { toFormUrlEncoded } from "./parser.js";
import {
  SEARCH_URL,
  SEARCH_REFERER,
  SEARCH_DATA,
  BASE_HEADERS,
} from "./config.js";



export async function fetchSearchHtml() {

  const body = toFormUrlEncoded(SEARCH_DATA);

 
   const resp = await client.post(SEARCH_URL, body, {
    headers: {
      ...BASE_HEADERS,
      "Content-Type": "application/x-www-form-urlencoded",
      Referer: SEARCH_REFERER,
    },
    maxRedirects: 5,
  });

  const html = resp.data;

  if (
    typeof html === "string" &&
    /password/i.test(html) &&
    /login/i.test(html)
  ) {
    throw new Error("LAABSearch returned login page – login probably failed.");
  }

  return html;
}