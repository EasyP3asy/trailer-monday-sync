// db/pool.js
// Shared pg Pool — one instance per process, imported by all repository files.

import pg from 'pg';
import { PG_USER, PG_HOST, PG_DATABASE, PG_PASSWORD, PG_PORT } from '../config.js';

const { Pool } = pg;

export const pool = new Pool({
  user: PG_USER, 
  host: PG_HOST, 
  database: PG_DATABASE,
  password: PG_PASSWORD, 
  port: PG_PORT, 
  ssl: false,
});