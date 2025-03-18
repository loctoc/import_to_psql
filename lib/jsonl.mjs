import { getTableConfig } from "./getTableConfigJSONL.mjs";
import fs from 'fs';
import readline from 'readline';

import moment from 'moment';

function formatElapsed(startTime) {
  const elapsed = Date.now() - startTime;
  if (elapsed < 1000) return `${elapsed}ms`;
  return `${(elapsed / 1000).toFixed(2)}s`;
}

export async function parseAndTransformJSONL(filePath, tableConfigFile, timezone, callBack, onComplete) {
  try {
    console.log(`\n📊 [${new Date().toISOString()}] Reading JSONL file...`);

    let header = [];
    let currentSheet = '';
    const transformedData = [];
    let startTime = Date.now();
    const noOfRows = {}
    await processFileLines(filePath, async (line, lineNumber) => {
      if (line.trim() === '') {
        return;
      }
      try {
        const json = JSON.parse(line);
        const currentSheetName = json.sheet;
        noOfRows[currentSheetName] = noOfRows[currentSheetName] || 0;
        noOfRows[currentSheetName]++;
        if (header.length === 0) {
          header = getTableConfig(Object.keys(json), tableConfigFile);
        }
        if (currentSheetName !== currentSheet && noOfRows[currentSheet] > 0) {
          await callBack({
            event: 'swap',
            columns: header
              .filter(c => !c.skip)
              .map(c => c.sqlColumn || sanitizeColumnName(c.header)),
            sheetName: currentSheet,
            transformedData: transformedData.splice(0),
            tableConfig: header,
            summary: {
              totalRows: transformedData.length,
              processedRows: transformedData.length,
              skippedRows: 0,
              emptyRows: 0,
              elapsed: formatElapsed(startTime)
            }
          });
          header = getTableConfig(Object.keys(json), tableConfigFile);
          startTime = Date.now();
        }
        // Commit every 50K rows to reduce memory usage
        if (transformedData.length === 50000) {
          await callBack({
            event: (currentSheetName !== currentSheet || noOfRows[currentSheetName] <= 50001) ? 'create' : 'insert',
            columns: header
              .filter(c => !c.skip)
              .map(c => c.sqlColumn || sanitizeColumnName(c.header)),
            sheetName: currentSheet,
            transformedData: transformedData.splice(0),
            tableConfig: header,
            summary: {
              totalRows: transformedData.length,
              processedRows: transformedData.length,
              skippedRows: 0,
              emptyRows: 0,
              elapsed: formatElapsed(startTime),
              total: noOfRows[currentSheet]
            }
          });
          header = getTableConfig(Object.keys(json), tableConfigFile);
          startTime = Date.now();
        }
        const transformedRow = header.map(col => {
          const val = json[col.header];
          if (col.fieldType === 'timestamp' && val) {
            try {
              const date = moment.tz(val, 'YYYY-MM-DD HH:mm', timezone);
              return date.isValid() ? date.toDate() : null;
            } catch (error) {
              console.warn(`Invalid date value: ${val}`);
              return null;
            }
          } else if (col.fieldType === 'number' && val !== null) {
            return Number(val);
          }
          return val;
        });
        transformedData.push(transformedRow);
        currentSheet = currentSheetName;
      } catch (error) {
        console.error(`⚠️ [${new Date().toISOString()}] Error processing JSONL:`, {
          message: error.message,
          stack: error.stack,
          line: line
        });
      }
    }, () => console.log(`File completed`));
    await callBack({
      columns: header
        .filter(c => !c.skip)
        .map(c => c.sqlColumn || sanitizeColumnName(c.header)),
      event: 'swap',
      sheetName: currentSheet,
      transformedData: transformedData.splice(0),
      tableConfig: header,
      summary: {
        totalRows: transformedData.length,
        processedRows: transformedData.length,
        skippedRows: 0,
        emptyRows: 0,
        elapsed: formatElapsed(startTime)
      }
    });
    onComplete();
  } catch (error) {
    console.error(`⚠️ [${new Date().toISOString()}] Error processing JSONL:`, {
      message: error.message,
      stack: error.stack,
      file: filePath
    });
    throw new Error(`Failed to process JSONL: ${error.message}\n${error.stack}`);
  }
}



/**
 * Reads a file line by line using a for...await...of loop and processes each line.
 *
 * @param {string} filePath The path to the file to read.
 * @param {function(string, number): void} lineProcessor A function that takes a line and its index as arguments.  It's called for each line in the file.
 * @returns {Promise<void>} A promise that resolves when the entire file has been read and processed.  Rejects if there's an error reading the file.
 */
async function processFileLines(filePath, lineProcessor, onComplete) {
  try {
    const fileStream = fs.createReadStream(filePath);

    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity, // Recognize all instances of CR LF as single line breaks.
    });

    let lineNumber = 0;
    for await (const line of rl) {
      try {
        await lineProcessor(line, lineNumber); // Call the processor function for each line.
      } catch (error) {
        console.error(`Error processing line ${lineNumber + 1}:`, error);
        // You might want to handle errors differently, like re-throwing or logging.
        // depending on your use case.  Re-throwing would likely cause the whole
        // process to stop.  Logging and continuing would allow it to move on to the
        // next line.
      }
      lineNumber++;
    }

    // Optional: Handle the end of the file explicitly.  The for...await...of loop
    // will exit automatically when the file stream is exhausted.
    rl.on('close', () => {
      onComplete();
    });

  } catch (error) {
    console.error('Error reading file:', error);
    throw error; // Re-throw the error to signal that the process failed.
  }
}