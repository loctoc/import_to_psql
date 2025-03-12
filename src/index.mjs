#!/usr/bin/env node

import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { parseAndTransformXLSXAllSheets } from '../lib/xlsx.mjs';
import { initializeDB, createTempTable, insertBatch, swapTables, closeDB } from '../lib/db.mjs';
import { sendSlackNotification } from '../lib/notifications.mjs';
import moment from 'moment';

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
      describe: 'Path to input Excel or CSV file',
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
      // if (!fs.existsSync(argv.tableConfig)) {
      //   throw new Error(`Table config file not found: ${argv.tableConfig}`);
      // }
      // Validate file extension
      const ext = path.extname(argv.inputFile).toLowerCase();
      if (!['.csv', '.xlsx', '.xls'].includes(ext)) {
        throw new Error(`Unsupported file type: ${ext}. Only .csv, .xlsx, and .xls files are supported`);
      }
      return true;
    })
    .argv;

  try {
    // Initialize database connection first
    console.log(`\n🔌 [${new Date().toISOString()}] Initializing database connection...`);
    initializeDB(process.env.DATABASE_URL);

    // 2. Read and validate table configuration
    console.log(`📋 [${new Date().toISOString()}] Loaded table configuration`);
    console.log(`🕒 [${new Date().toISOString()}] Using timezone: ${argv.timezone}`);
    console.log(`📦 [${new Date().toISOString()}] Batch size: ${argv.batchSize}`);

    // 3. Parse and transform data based on file type
    await parseAndTransformXLSXAllSheets(
      argv.inputFile,
      argv.tableConfig,
      argv.timezone,
      async ({ columns, transformedData, summary, tableConfig, sheetId }) => {
        // 4. Create temporary table with timestamp-suffixed indexes
        const timestamp = moment().format('YYYYMMDDHHMMSS');
        const tableName = argv.table + `_sheet${sheetId}`;
        const { tmpTableName } = await createTempTable(tableName, columns, tableConfig, timestamp);
        const {emptyRows = 0, skippedRows = 0, totalRows = 0} = summary;
        // 5. Insert data in batches
        console.log('\nInserting data...');
        let insertedRows = 0;
        const startTime = Date.now();

        for (let i = 0; i < transformedData.length; i += argv.batchSize) {
          const batch = transformedData.slice(i, i + argv.batchSize);
          await insertBatch(tmpTableName, columns, batch);
          insertedRows += batch.length;
        }

        // 6 & 7. Handle table swap based on truncate option
        if (argv.truncate) {
          console.log('\nSwapping tables...');
          await swapTables(tmpTableName, tableName, true);
        } else {
          console.log('\nMerging data...');
          await swapTables(tmpTableName, tableName, false);
        }

        const summaryData = {
          inputFile: path.basename(argv.inputFile),
          tableName: tableName,
          totalRows: totalRows + emptyRows + skippedRows,
          validRows: insertedRows,
          emptyRows,
          skippedRows,
          duration: ((Date.now() - startTime) / 1000).toFixed(2),
          sheetName: sheetId
        };

        const successMessage = `✅ Successfully imported ${insertedRows} rows into ${tableName}`;
        console.log(`\n${successMessage}`);
        await sendSlackNotification(argv.slackNotifyUrl, successMessage, summaryData);
      }
    )

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
  }
}

main(); 