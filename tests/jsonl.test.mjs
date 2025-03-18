import { describe, test, expect } from 'vitest';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('getTableConfigForAWorkSheet', () => {
  test('should handle duplicate column names correctly', () => {
    const tableConfigFile = path.join(__dirname, 'table-config.json');
    const tableConfig = getTableConfigForAWorkSheet({}, tableConfigFile);
    expect(tableConfig).toMatchSnapshot();
  });
});