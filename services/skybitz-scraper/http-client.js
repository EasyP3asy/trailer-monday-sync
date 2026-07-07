// skybitz-http-client.js
//
// One shared axios client + cookie jar. This is the only file that creates
// the client — both skybitz-auth.js (which logs in and gets cookies) and
// skybitz-fetcher.js (which uses those cookies to search) import THIS SAME
// instance, so the cookies set during login are visible during the search
// request. If each file made its own client, cookies wouldn't carry over
// and every search would look like an unauthenticated request.

import axios from "axios";
import { wrapper } from "axios-cookiejar-support";
import { CookieJar } from "tough-cookie";

export const jar = new CookieJar();

export const client = wrapper(
  axios.create({
    jar,    
  })
);