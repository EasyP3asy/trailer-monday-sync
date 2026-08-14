// config.js
// All environment variables in one place. Every other file imports
// from here instead of reading process.env directly — so if a variable
// name ever changes, you fix it in one place, not across every file.

import 'dotenv/config';

// ---- ORBCOMM ----
export const ORBCOMM_USER_ID = process.env.ORBCOMM_USER_ID;
export const ORBCOMM_USER_PASSWORD   = process.env.ORBCOMM_USER_PASSWORD;

// ---- Telegram ----
export const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
export const TELEGRAM_CHAT_ID   = process.env.TELEGRAM_USER_ID;
export const WEBHOOK_BASE_URL = process.env.WEBHOOK_BASE_URL;
export const TELEGRAM_SYNC_TOPIC_ID = process.env.TELEGRAM_SYNC_TOPIC_ID;

// ---- SkyBitz XML API ----
export const SKYBITZ_BASE     = process.env.SKYBITZ_BASE;
export const SKYBITZ_CUSTOMER = process.env.SKYBITZ_CUSTOMER;
export const SKYBITZ_PASSWORD = process.env.SKYBITZ_PASSWORD;
export const SKYBITZ_VERSION  = process.env.SKYBITZ_VERSION ;

// ---- SkyBitz Scraper ----
export const SKYBITZ_USER = process.env.SKYBITZ_USER;
export const SKYBITZ_PASS = process.env.SKYBITZ_PASS;

// ---- Samsara ----
export const SAMSARA_API_TOKEN = process.env.SAMSARA_API_TOKEN;
export const SAMSARA_BASE_URL  = process.env.SAMSARA_BASE_URL;

// ---- Monday.com ----
export const MONDAY_API_TOKEN       = process.env.MONDAY_API_TOKEN;
export const TRAILER_BOARD_ID       = process.env.TRAILER_BOARD_ID;
export const TRAILER_BOARD_GROUP_ID = process.env.TRAILER_BOARD_GROUP_ID;

// ---- PostgreSQL ----
export const PG_USER     = process.env.PG_USER;
export const PG_HOST     = process.env.PG_HOST;
export const PG_DATABASE = process.env.PG_DATABASE;
export const PG_PASSWORD = process.env.PG_PASSWORD;
export const PG_PORT     = process.env.PG_PORT || 5432;

// ---- Google Sheets ----
export const GOOGLE_PRIVATE_KEY  = process.env.GOOGLE_PRIVATE_KEY;
export const GOOGLE_CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL;
export const SPREADSHEET_ID      = process.env.SPREADSHEET_ID;
export const SHEET_NAME          = process.env.SHEET_NAME;

// ---- Server ----
export const PORT = process.env.PORT || 3003;