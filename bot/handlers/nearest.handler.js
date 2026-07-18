// bot/handlers/nearest.handler.js
//
// Handles the /nearest command.
// Usage: /nearest 33.1234 -97.5678
// Returns the 50 closest trailers sorted by distance in miles.

import { getAllTrailers }    from '../../db/trailer.repository.js';
import { vincentyDistance }  from '../../utils/geo.utils.js';
import { formatIdleDuration } from '../../services/idle-tracker.service.js';

const METERS_PER_MILE  = 1609.344;
const MAX_RESULTS      = 25;
const MAX_MESSAGE_CHARS = 3500; // buffer under Telegram's 4096 char limit

// ---- Group blocks so each message's code-block body stays under the char limit ----
function chunkBlocksByLength(blocks, maxChars) {
  const chunks = [];
  let current = [];
  let currentLen = 0;

  for (const block of blocks) {
    const addedLen = block.length + 2; // +2 for the '\n\n' separator between blocks
    if (current.length > 0 && currentLen + addedLen > maxChars) {
      chunks.push(current);
      current = [];
      currentLen = 0;
    }
    current.push(block);
    currentLen += addedLen;
  }
  if (current.length) chunks.push(current);
  return chunks;
}

export function registerNearestHandler(bot) {
  bot.onText(/\/nearest (.+)/, async (msg, match) => {
    console.log('Nearest handler triggered:', match[1]);
    const chatId = msg.chat.id;

    // ---- Parse coordinates ----
    const parts = match[1].trim().split(/[\s,]+/);
    const lat   = parseFloat(parts[0]);
    const lon   = parseFloat(parts[1]);

    if (isNaN(lat) || isNaN(lon)) {
      await bot.sendMessage(chatId,
        '❌ Invalid coordinates.\n\nUsage: /nearest 33.1234 -97.5678'
      );
      return;
    }

    if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
      await bot.sendMessage(chatId,
        '❌ Coordinates out of range.\n\nLatitude: -90 to 90\nLongitude: -180 to 180'
      );
      return;
    }

    await bot.sendMessage(chatId, '🔍 Searching for nearest trailers...');

    try {
      // ---- Fetch all trailers from DB ----
      const trailers = await getAllTrailers();

      if (!trailers.length) {
        await bot.sendMessage(chatId, '❌ No trailers found in database.');
        return;
      }

      // ---- Calculate distance from given point to each trailer ----
      const withDistance = trailers.map(t => ({
        ...t,
        distanceMiles: vincentyDistance(lat, lon, t.latitude, t.longitude) / METERS_PER_MILE,
      }));

      // ---- Sort by distance ascending, take top 50 ----
      const nearest = withDistance
        .sort((a, b) => a.distanceMiles - b.distanceMiles)
        .slice(0, MAX_RESULTS);

      // ---- Format each trailer as a labeled block ----
      const blocks = nearest.map((t, i) => {
        const distance = t.distanceMiles < 1
          ? `${(t.distanceMiles * 5280).toFixed(0)} ft`
          : `${t.distanceMiles.toFixed(1)} mi`;
        const idle   = formatIdleDuration(t.idle_duration);
        const latLon = `${t.latitude}, ${t.longitude}`;

        return `${i + 1}. ${t.trailer_number} — ${distance}\n` +
               `================\n` +
               `Location : ${t.full_address || 'Unknown location'}\n` +
               `Lat&Lon  : ${latLon}\n` +
               `IDLE     : ${idle}`;
      });

      const header = `🚛 *${nearest.length} Nearest Trailers*\n` +
                     `📌 From: ${lat.toFixed(4)}, ${lon.toFixed(4)}\n\n`;

      // ---- Send one message per chunk, each as its own code block ----
      const chunks = chunkBlocksByLength(blocks, MAX_MESSAGE_CHARS);
      for (let i = 0; i < chunks.length; i++) {
        const body = '```\n' + chunks[i].join('\n\n') + '\n```';
        const text = i === 0 ? header + body : body;
        await bot.sendMessage(chatId, text, { parse_mode: 'Markdown' });
      }

    } catch (err) {
      console.error('nearest handler error:', err);
      await bot.sendMessage(chatId, `❌ Error fetching trailers: ${err.message}`);
    }
  });
}