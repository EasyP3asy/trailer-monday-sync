// bot/bot.js
// Telegram bot entry point.
// Placeholder — wire up handlers here when bot is ready.

 import TelegramBot from 'node-telegram-bot-api';
 import { TELEGRAM_BOT_TOKEN } from '../config.js';
 import { registerStartHandler }  from './handlers/start.handler.js';
 import { registerStatusHandler } from './handlers/status.handler.js';

export const bot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: true });



bot.on('polling_error', (error) => {
  console.error('Polling error:', error.message);
});

bot.on('message', (msg) => {
  console.log('Message received:', msg.text);
});



export function startBot() {
  // const bot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: true });
   registerStartHandler(bot);
   registerStatusHandler(bot);
   console.log('✅ Telegram bot started');
  //console.log('ℹ️  Bot not yet configured — see bot/bot.js');
}

