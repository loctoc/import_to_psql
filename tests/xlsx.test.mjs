import { describe, test, expect } from 'vitest';
import XLSX from 'xlsx';
import path from 'path';
import { fileURLToPath } from 'url';
import { getTableConfigForAWorkSheet } from '../lib/getTableConfig.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('parseAndTransformXLSX', () => {
  test('should parse and transform XLSX data', () => {
    // TODO: Implement test
  })
});

describe('getTableConfigForAWorkSheet', () => {
  test('should handle duplicate column names correctly', () => {
    // Load the test Excel file
    const workbook = XLSX.readFile(path.join(__dirname, 'duplicae-columns-test.xlsx'));
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];

    // Get table config
    const tableConfig = getTableConfigForAWorkSheet(worksheet, null);

    // Expected column names after handling duplicates
    const expectedColumns = [
      'Form Name',
      'Submission Number',
      'Sent At',
      'Sender Name',
      'Sender Identifier',
      'Sender Department',
      'Sender Designation',
      'Sender Location',
      'Sender Division',
      'Sender Sub Division',
      'Current Status',
      'Remarks',
      'Product1',
      'Evidence_1',
      'Product2',
      'Evidence_2',
      'Product3',
      'Evidence_3'
    ];

    // Test that we have the correct number of columns
    expect(tableConfig.length).toBe(expectedColumns.length);

    // Test that each column has the expected name
    tableConfig.forEach((config, index) => {
      expect(config.sqlColumn).toBe(expectedColumns[index]);
    });

    // Specifically test the duplicate 'Evidence' columns
    const evidenceColumns = tableConfig.filter(config => 
      config.header === 'Evidence'
    );
    
    expect(evidenceColumns.length).toBe(3);
    expect(evidenceColumns[0].sqlColumn).toBe('Evidence_1');
    expect(evidenceColumns[1].sqlColumn).toBe('Evidence_2');
    expect(evidenceColumns[2].sqlColumn).toBe('Evidence_3');
    expect(tableConfig).toMatchSnapshot();
  });
  test.skip('Critical_Machine_Checks_Form_responses.xlsx real test case 1', () => {
    // Load the test Excel file
    const workbook = XLSX.readFile(path.join(__dirname, 'Real_TestCase_1.xlsx'));
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const tableConfig = getTableConfigForAWorkSheet(worksheet, path.join(__dirname, 'table-config.json'));
    expect(tableConfig).toMatchSnapshot();
  })
});