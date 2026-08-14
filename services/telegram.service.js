// services/telegram.service.js
// Sends alert messages to Telegram.

import { TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID } from '../config.js';

function escapeMarkdown(t) {
  return String(t).replace(/([_*[\]()`])/g, '\\$1');
}

export async function sendErrorToTelegram(messageText,threadId = null) {
  const message = `🚨 *Alert!* Trailer-Monday-Sync🚨\n\n${escapeMarkdown(messageText)}`;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  try {

    const params = { 
      chat_id: TELEGRAM_CHAT_ID, 
      text: messageText, 
      parse_mode: 'Markdown' 
    }

    if(threadId){
      params.message_thread_id = threadId;
    }


    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`Telegram API error: ${await res.text()}`);
    console.log('✅ Telegram alert sent');
  } catch (err) {
    console.error('❌ Failed to send Telegram message:', err);
  }
}




export async function sendMessageToTelegram(messageText,threadId) { 
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  try {

    const params = { 
      chat_id: TELEGRAM_CHAT_ID, 
      text: messageText, 
      parse_mode: 'Markdown' 
    }

    if(threadId){
      params.message_thread_id = threadId;
    }


    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`Telegram API error: ${await res.text()}`);
    console.log('✅ Telegram alert sent');
  } catch (err) {
    console.error('❌ Failed to send Telegram message:', err);
  }
}