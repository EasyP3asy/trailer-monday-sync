// bot/bot.js
// Telegram bot entry point.
// Placeholder — wire up handlers here when bot is ready.

 import TelegramBot from 'node-telegram-bot-api';
 import { TELEGRAM_BOT_TOKEN ,WEBHOOK_BASE_URL  } from '../config.js';
 import { registerStartHandler }  from './handlers/start.handler.js';
 import { registerStatusHandler } from './handlers/status.handler.js';
 import { registerNearestHandler } from './handlers/nearest.handler.js';

export const bot = new TelegramBot(TELEGRAM_BOT_TOKEN);






export async function startBot() {
  
   registerStartHandler(bot);
   registerStatusHandler(bot);
   registerNearestHandler(bot);

    const webhookUrl = `${WEBHOOK_BASE_URL}/tgwebhook`;
    await bot.setWebHook(webhookUrl);
   console.log('✅ Telegram bot started');
  //console.log('ℹ️  Bot not yet configured — see bot/bot.js');
}

