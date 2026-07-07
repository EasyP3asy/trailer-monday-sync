// server.js
// Entry point — starts express, DB check, cron job, and bot.

import express from 'express';
import { ensureTableExists }  from './db/trailer.repository.js';
import { sendErrorToTelegram } from './services/telegram.service.js';
import { startSyncJob }        from './jobs/sync.job.js';
import { startBot }            from './bot/bot.js';
import { PORT }                from './config.js';

const app = express();
app.use(express.json());

app.get('/health', (req, res) => res.status(200).json({ status: 'ok' }));
app.get('/',       (req, res) => res.status(200).json({ ok: true }));


(async () => {
  try {
    //await ensureTableExists({ strict: true });
    app.listen(PORT, () => console.log(`🚀 Server running on PORT ${PORT}`));
    startSyncJob();
   // startBot();
  } catch (e) {
    console.error('Startup failed:', e);
    try { await sendErrorToTelegram(`Startup failed: ${e.message}`); } catch (_) {}
    process.exit(1);
  }
})();