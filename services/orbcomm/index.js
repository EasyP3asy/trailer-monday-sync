import { parseAssets } from './parser.js';
// you can make api call once every 5 minutes 

import {login} from './auth.js';
import {fetchAssetsData} from './fetcher.js';


export async function fetchOrbcommAssets() {
  try{
    const accessCredentials = await login(); // 1) getAcessToken 
    const data = await fetchAssetsData(accessCredentials);
    return parseAssets(data);  // parses response 
    
  }catch(err){
      throw err;
  }
}

