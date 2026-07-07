// utils/geo.utils.js
export function extractState(address) {
  if (!address) return null;
  const re = /(?:^|,\s*)(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)(?=,|$|\s\d{5})/;
  const match = address.match(re);
  return match ? match[1] : null;
}

export function vincentyDistance(lat1, lon1, lat2, lon2) {
  const toRad = d => (d * Math.PI) / 180;
  const a = 6378137.0, f = 1 / 298.257223563, b = a * (1 - f);
  const phi1 = toRad(lat1), phi2 = toRad(lat2), L = toRad(lon2 - lon1);
  const U1 = Math.atan((1-f)*Math.tan(phi1)), U2 = Math.atan((1-f)*Math.tan(phi2));
  const sU1=Math.sin(U1),cU1=Math.cos(U1),sU2=Math.sin(U2),cU2=Math.cos(U2);
  let lam=L,lp,sS,cS,sig,sA,c2A,c2SM;
  for(let i=0;i<200;i++){
    const sL=Math.sin(lam),cL=Math.cos(lam);
    sS=Math.sqrt((cU2*sL)**2+(cU1*sU2-sU1*cU2*cL)**2);
    if(sS===0)return 0;
    cS=sU1*sU2+cU1*cU2*cL; sig=Math.atan2(sS,cS);
    sA=(cU1*cU2*sL)/sS; c2A=1-sA**2;
    c2SM=c2A!==0?cS-(2*sU1*sU2)/c2A:0;
    const C=(f/16)*c2A*(4+f*(4-3*c2A)); lp=lam;
    lam=L+(1-C)*f*sA*(sig+C*sS*(c2SM+C*cS*(-1+2*c2SM**2)));
    if(Math.abs(lam-lp)<1e-12)break; if(i===199)return NaN;
  }
  const uSq=c2A*((a*a-b*b)/(b*b));
  const A=1+(uSq/16384)*(4096+uSq*(-768+uSq*(320-175*uSq)));
  const B=(uSq/1024)*(256+uSq*(-128+uSq*(74-47*uSq)));
  const dS=B*sS*(c2SM+(B/4)*(cS*(-1+2*c2SM**2)-(B/6)*c2SM*(-3+4*sS**2)*(-3+4*c2SM**2)));
  return b*A*(sig-dS);
}