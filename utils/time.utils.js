// utils/time.utils.js
export function formatToEasternTime(input) {
  const date = new Date(input);
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    month: 'short', day: 'numeric', year: 'numeric',
    hour: 'numeric', minute: '2-digit',
    hour12: true, timeZoneName: 'short',
  }).format(date);
}

export function diffToText(earlier, later = new Date()) {
  const start = earlier instanceof Date ? earlier : new Date(earlier);
  const end   = later   instanceof Date ? later   : new Date(later);
  let diffMs  = end - start;
  if (diffMs < 0) diffMs = -diffMs;

  const totalMinutes = Math.floor(diffMs / (1000 * 60));
  const days  = Math.floor(totalMinutes / (60 * 24));
  const hours = Math.floor((totalMinutes % (60 * 24)) / 60);
  const mins  = totalMinutes % 60;

  const parts = [];
  if (days)  parts.push(`${days} day${days   !== 1 ? 's' : ''}`);
  if (hours) parts.push(`${hours} hour${hours !== 1 ? 's' : ''}`);
  if (mins || parts.length === 0) parts.push(`${mins} minute${mins !== 1 ? 's' : ''}`);
  return parts.join(' ') + ' ago';
}