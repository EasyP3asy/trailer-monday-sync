// jobs/sync.job.js
// Cron job — knows WHEN to run the sync, delegates WHAT to the controller.

import cron from 'node-cron';
import { runSync } from '../controllers/sync.controller.js';

let isRunning = false;

export function startSyncJob() {
  cron.schedule(
    '*/30 * * * *',
    async () => {
      if (isRunning) {
        console.log('Cron: previous run still in progress, skipping');
        return;
      }
      isRunning = true;
      console.log('Cron: starting sync at', new Date().toISOString());
      try {
        await runSync();
        console.log('Cron: sync finished');
      } catch (err) {
        console.error('Cron: sync error:', err);
      } finally {
        isRunning = false;
      }
    },
    { timezone: 'America/New_York' }
  );

  console.log('✅ Sync job scheduled (every 30 minutes)');
}