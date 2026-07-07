




export function parseAssets(resBody) {

    const assetsArray = resBody?.data;
    try{

        if(!assetsArray) throw new Error("ORBCOMM : No Response Data");
        else if(assetsArray.length == 0) throw new Error("ORBCOMM : Empty list of assets");
        
        const filteredAssets = assetsArray.map((asset)=>{
                return {
                    assetId : asset?.assetStatus?.assetName ?? null,
                    address : asset?.positionStatus?.address ?? null,  
                    batteryStatus : asset?.assetStatus?.batteryStatus ?? null,  
                    city : asset?.positionStatus?.city ?? null,
                    country : asset?.positionStatus?.country ?? null,                
                    latitude : asset?.positionStatus?.latitude ?? null,
                    longitude : asset?.positionStatus?.longitude ?? null,
                    obsTime : asset?.assetStatus?.messageStamp ?? null,
                    messageReceivedTime : asset?.assetStatus?.messageReceivedStamp ?? null,
                    messageType : asset?.assetStatus?.messageType ?? null,
                    moving : asset?.impactStatus?.moving ?? null,
                    serialNum : asset?.assetStatus?.deviceSN ?? null,
                    street : asset?.positionStatus?.street ?? null,                                      // no such property in ORBCOMM
                    state : asset?.positionStatus?.state ?? null,                
                    zipCode : asset?.positionStatus?.zipCode ?? null,
                };
        });

        return filteredAssets;

    }catch(err){
      throw err;        
    }


     


}