import { parseAndTransformXLSXAllSheets } from './xlsx.mjs';
import { createTempTable, insertBatch, swapTables } from './db.mjs';
import moment from 'moment';
import { sendSlackNotification } from './notifications.mjs';
import path from 'path';

export default async function handleXLSX(argv) {
  return new Promise((resolve, _reject) => {
    // 3. Parse and transform data based on file type
    parseAndTransformXLSXAllSheets(
      argv.inputFile,
      argv.tableConfig,
      argv.timezone,
      async ({ columns, transformedData, summary, tableConfig, sheetId }) => {
        // 4. Create temporary table with timestamp-suffixed indexes
        const timestamp = moment().format('YYYYMMDDHHMMSS');
        const tableName = argv.table + `_sheet${sheetId}`;
        const { tmpTableName } = await createTempTable(tableName, columns, tableConfig, timestamp);
        const { emptyRows = 0, skippedRows = 0, totalRows = 0 } = summary;
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
      },
      resolve
    );
  });
}