// utils/array.utils.js
export function toArray(v) {
  return v == null ? [] : Array.isArray(v) ? v : [v];
}