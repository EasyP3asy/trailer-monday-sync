// skybitz-auth.js
//
// One job: log in and leave fresh cookies in the shared jar (imported from
// skybitz-http-client.js). Nothing returned here matters — the side effect
// (cookies in the jar) is the point. skybitz-fetcher.js relies on this
// having run first.
// Second job : it activates the sessionID to the url that we are using

import { client ,jar } from "./http-client.js";
import { toFormUrlEncoded } from "./parser.js";
import {
  LOGIN_URL,
  ACTIVATE_URL,
  LOGIN_REFERER,
  BASE_HEADERS,
  SKYBITZ_USER,
  SKYBITZ_PASS,
  LOGIN_REFERER_ACTIVATION,
} from "./config.js";






export async function login() {
 
  const loginData = {
    strUserName: SKYBITZ_USER,
    strPassword: SKYBITZ_PASS,
    go: "GO",
  };

 

  const body = toFormUrlEncoded(loginData);

  const loginResp = await client.post(LOGIN_URL, body, {
    headers: {
      ...BASE_HEADERS,
      "Content-Type": "application/x-www-form-urlencoded",
      Referer: LOGIN_REFERER,
    },
    maxRedirects: 5, 
  });


   
    
console.log("Status:", loginResp.status);


const activationCookies = await jar.getCookies("https://insight.skybitz.com");
const idke = activationCookies.find(c => c.key === "idke")?.value;
const udke = activationCookies.find(c => c.key === "udke")?.value;


  const activationResp = await client.post(ACTIVATE_URL, toFormUrlEncoded({ idke, udke }), {
    headers: {
      ...BASE_HEADERS,
      "Content-Type": "application/x-www-form-urlencoded",
      Referer: LOGIN_REFERER_ACTIVATION,
    },
    maxRedirects: 5,
  });








  console.log("Login POST status:", activationResp.status);
 
 
}