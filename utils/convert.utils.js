// utils/convert.utils.js
export function kphToMph(kph) {
  if (typeof kph !== 'number' || !Number.isFinite(kph))
    throw new TypeError('kph must be a finite number');
  return kph * 0.621371;
}
export function kphToMphRounded(kph, decimals = 2) {
  const p = 10 ** decimals;
  return Math.round(kphToMph(kph) * p) / p;
}