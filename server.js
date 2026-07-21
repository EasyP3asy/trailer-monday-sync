// server.js
// Entry point — starts express, DB check, cron job, and bot.

import express from 'express';
import { ensureTableExists }  from './db/trailer.repository.js';
import { sendErrorToTelegram } from './services/telegram.service.js';
import { startSyncJob }        from './jobs/sync.job.js';
import { bot, startBot }            from './bot/bot.js';
import { PORT }                from './config.js';



const app = express();
app.use(express.json());

app.get('/health', (req, res) => res.status(200).json({ status: 'ok' }));
app.get('/',       (req, res) => res.status(200).json({ ok: true }));

// Telegram sends POST requests to this URL
app.post('/tgwebhook', (req, res) => {
  console.log('Webhook received:', JSON.stringify(req.body));
    res.sendStatus(200);
    bot.processUpdate(req.body);  
});

app.use((err, req, res, next) => {

   console.error('Express error:', {
    method: req.method,        // GET, POST etc
    url: req.url,              // which endpoint
    errorType: err.type,       // entity.parse.failed etc
    errorMessage: err.message, // the actual error
    body: req.body,            // what was parsed (might be empty)
    rawBody: req.headers['content-type'], // what content type was sent
    ip: req.ip,                // who sent it
  });

  res.sendStatus(200);
});




(async () => {
  try {
    await ensureTableExists({ strict: true });
    app.listen(PORT, () => console.log(`🚀 Server running on PORT ${PORT}`));
    startSyncJob();
    await startBot();
  } catch (e) {
    console.error('Startup failed:', e);
    try { await sendErrorToTelegram(`Startup failed: ${e.message}`); } catch (_) {}
    process.exit(1);
  }
})();