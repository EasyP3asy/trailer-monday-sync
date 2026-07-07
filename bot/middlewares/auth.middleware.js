// bot/middlewares/auth.middleware.js
// Checks if a Telegram user is allowed to use the bot.
// Placeholder — implement when bot is ready.

const ALLOWED_CHAT_IDS = [
  // process.env.TELEGRAM_USER_ID,
];

export function isAuthorized(chatId) {
  return ALLOWED_CHAT_IDS.includes(String(chatId));
}