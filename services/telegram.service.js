// services/telegram.service.js
// Sends alert messages to Telegram.

import { TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID } from '../config.js';

function escapeMarkdown(t) {
  return String(t).replace(/([_*[\]()`])/g, '\\$1');
}

export async function sendErrorToTelegram(messageText) {
  const message = `🚨 *Alert!* Trailer-Monday-Sync🚨\n\n${escapeMarkdown(messageText)}`;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: 'Markdown' }),
    });
    if (!res.ok) throw new Error(`Telegram API error: ${await res.text()}`);
    console.log('✅ Telegram alert sent');
  } catch (err) {
    console.error('❌ Failed to send Telegram message:', err);
  }
}




export async function sendMessageToTelegram(messageText) { 
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: messageText, parse_mode: 'Markdown' }),
    });
    if (!res.ok) throw new Error(`Telegram API error: ${await res.text()}`);
    console.log('✅ Telegram alert sent');
  } catch (err) {
    console.error('❌ Failed to send Telegram message:', err);
  }
}