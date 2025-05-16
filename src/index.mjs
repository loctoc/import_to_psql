#!/usr/bin/env node

import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

import { sendSlackNotification } from '../lib/notifications.mjs';
import { initializeDB, closeDB } from '../lib/db.mjs';
import handleXLSX from '../lib/handleXLSX.mjs';
import handleJSONL from '../lib/handleJSONL.mjs';
// Load environment variables first
dotenv.config();

if (!process.env.DATABASE_URL) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function main() {
  // 1. Parse CLI arguments
  const argv = yargs(hideBin(process.argv))
    .option('input-file', {
      describe: 'Path to input Excel or CSV or JSONL file',
      type: 'string',
      demandOption: true
    })
    .option('table', {
      describe: 'Target PostgreSQL table (format: schema.table)',
      type: 'string',
      demandOption: true
    })
    .option('table-config', {
      describe: 'JSON file containing table configuration',
      type: 'string',
      demandOption: true
    })
    .option('timezone', {
      describe: 'Timezone for date parsing (e.g., Asia/Kolkata)',
      type: 'string',
      demandOption: true
    })
    .option('batch-size', {
      describe: 'Number of records per batch insert',
      type: 'number',
      default: 5000
    })
    .option('truncate', {
      describe: 'Truncate table before import',
      type: 'boolean',
      default: false
    })
    .option('slack-notify-url', {
      describe: 'Slack webhook URL for notifications',
      type: 'string'
    })
    .check((argv) => {
      // Validate file exists
      if (!fs.existsSync(argv.inputFile)) {
        throw new Error(`Input file not found: ${argv.inputFile}`);
      }
      // Validate table config exists
      if (!fs.existsSync(argv.tableConfig)) {
        throw new Error(`Table config file not found: ${argv.tableConfig}`);
      }
      // Validate file extension
      const ext = path.extname(argv.inputFile).toLowerCase();
      if (!['.csv', '.xlsx', '.xls', '.jsonl'].includes(ext)) {
        throw new Error(`Unsupported file type: ${ext}. Only .csv, .xlsx, .xls, and .jsonl files are supported`);
      }
      return true;
    })
    .argv;

  const ext = path.extname(argv.inputFile).toLowerCase();

  // Initialize database connection first
  console.log(`\n🔌 [${new Date().toISOString()}] Initializing database connection...`);
  initializeDB(process.env.DATABASE_URL);

  // 2. Read and validate table configuration
  console.log(`📋 [${new Date().toISOString()}] Loaded table configuration`);
  console.log(`🕒 [${new Date().toISOString()}] Using timezone: ${argv.timezone}`);
  console.log(`📦 [${new Date().toISOString()}] Batch size: ${argv.batchSize}`);

  try {
    if (ext === '.csv') {
      console.error('Not Yet supported');
    } else if (ext === '.jsonl') {
      await handleJSONL(argv);
    } else if (ext === '.xlsx') {
      await handleXLSX(argv);
    }
  } catch (error) {
    const errorMessage = `❌ Error importing data: ${error.message}`;
    console.error(`\n${errorMessage}`);
    try {
      await sendSlackNotification(argv.slackNotifyUrl, errorMessage, {
        error: error.message,
        stack: error.stack,
        inputFile: path.basename(argv.inputFile)
      });
    } catch (slackError) {
      console.error('Failed to send Slack notification:', slackError.message);
    }
    process.exit(1);
  } finally {
    try {
      await closeDB();
    } catch (dbError) {
      console.error('Error closing database connection:', dbError.message);
    }
    process.exit(0);
  }
}

main();
