


import {
       ORBCOMM_USER_ID ,
       ORBCOMM_USER_PASSWORD,
       ORBCOMM_BASE_URL
} from "./config.js";






export async function login() {
 
  const GENERATE_ACCESS_TOKEN_URL = `${ORBCOMM_BASE_URL}generateToken`;

  const body = {
    userName: ORBCOMM_USER_ID,
    password: ORBCOMM_USER_PASSWORD,       
  }; 


 

  const res = await fetch(GENERATE_ACCESS_TOKEN_URL, {
      method: 'POST',
      body:JSON.stringify(body),
      headers: {
        'Content-Type': 'application/json',        
      },
    });

    if(!res.ok) throw new Error(`ORBCOMM AUTH ERROR : ${res.statusText} (${res.status})`);

    const {data} = await res.json();

   
  


  return data;

 
}