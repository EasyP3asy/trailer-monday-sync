

import {
  ASSETS_LIST,  
  ORBCOMM_BASE_URL,
} from "./config.js";
import { sendErrorToTelegram } from "../telegram.service.js";


export async function fetchAssetsData(accessCredentials) {

   const ACCESS_TOKEN = accessCredentials.accessToken;
   
   const GET_ASSET_STATUS_URL = `${ORBCOMM_BASE_URL}getAssetStatus`;

   const body = {
        assetNames : ASSETS_LIST,
    };
    
    
   
   try{
        const res = await fetch(
                GET_ASSET_STATUS_URL, 
                {
                    method: 'POST',
                    body: JSON.stringify(body),
                    headers: {
                        'Authorization': ACCESS_TOKEN,
                        'Content-Type': 'application/json',        
                    },
                },
        );

        
        const resBody = await res.json();

        

        if(!res.ok) throw new Error(`Fetch Error!!! : ${resBody.message} (${res.status})`);


        
        
        return resBody;

        

    }catch(err){
         await sendErrorToTelegram(`⚠️ Orbcomm API failed: ${err.message}`);
         throw new Error(`Orbcomm API failed : ${err.message}`);        
    }





   
   
 

  return ;
}