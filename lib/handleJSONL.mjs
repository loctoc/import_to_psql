import { parseAndTransformJSONL } from './jsonl.mjs';
import { createTempTable, insertBatch, swapTables } from './db.mjs';
import moment from 'moment';
import { sendSlackNotification } from './notifications.mjs';
import path from 'path';

function waitInSecs(seconds) {
	  console.log(`\nMemory usage: ${process.memoryUsage().heapUsed / 1024 } KB`);
console.log(`\nMemory usage: ${process.memoryUsage().heapUsed / 1024 / 1024} MB`);
  return new Promise(resolve => setTimeout(resolve, seconds * 1000));
}

export default async function handleJSONL(argv) {
  return new Promise((resolve, _reject) => {
    parseAndTransformJSONL(
      argv.inputFile,
      argv.tableConfig,
      argv.timezone,
      async ({ event, columns, transformedData, summary, tableConfig, sheetName }) => {
        // 4. Create temporary table with timestamp-suffixed indexes
        const timestamp = moment().format('YYYYMMDDHHMMSS');
        const tableName = argv.table + `_${sheetName}`;
        const { tmpTableName } = await createTempTable(tableName, columns, tableConfig, timestamp, event);
        const { emptyRows = 0, skippedRows = 0, totalRows = 0 } = summary;
        // 5. Insert data in batches
        console.log('\nInserting data...');
        let insertedRows = 0;
        const startTime = Date.now();

        for (let i = 0; i < transformedData.length; i += argv.batchSize) {
          console.log(`\nInserting batch ${i / argv.batchSize + 1} of ${Math.ceil(transformedData.length / argv.batchSize)}`);
          const batch = transformedData.slice(i, i + argv.batchSize);
          await insertBatch(tmpTableName, columns, batch);
          insertedRows += batch.length;
        }
	      await waitInSecs(10);

        // 6 & 7. Handle table swap based on truncate option
        if (argv.truncate && event === 'swap') {
          console.log('\nSwapping tables...');
          await swapTables(tmpTableName, tableName, true);
        } else if (event === 'swap') {
          console.log('\nMerging data...');
          await swapTables(tmpTableName, tableName, false);
        }
        if (event === 'swap') {
          const summaryData = {
            inputFile: path.basename(argv.inputFile),
            tableName: tableName,
            totalRows: totalRows + emptyRows + skippedRows,
            validRows: insertedRows,
            emptyRows,
            skippedRows,
            duration: ((Date.now() - startTime) / 1000).toFixed(2),
            sheetName: sheetName
          };
          const successMessage = `✅ Successfully imported ${insertedRows} rows into ${tableName}`;
          console.log(`\n${successMessage}`);
          await sendSlackNotification(argv.slackNotifyUrl, successMessage, summaryData);
        }
      },
      resolve
    );
  });
}
