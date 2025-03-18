import fs from 'fs';

export function getTableConfigOverrides(tableConfigFile) {
  if (tableConfigFile && fs.existsSync(tableConfigFile)) {
    const tableConfig = JSON.parse(fs.readFileSync(tableConfigFile, 'utf-8'));
    return tableConfig
  }
  return {}
}

export function getTableConfig(headers, tableConfigFile) {
  const tableConfigOverrides = getTableConfigOverrides(tableConfigFile);

  // First get all headers and create base configs
  const tableConfig = headers
    .filter(a => a !== 'sheet')
    .map((header, idx) => {
      const colOverrides = tableConfigOverrides?.[header] ?? {};
      return {
        "header": header,
        "sqlColumn": header,
        "fieldType": "string",
        "primary": false,
        "notNull": false,
        "skip": false,
        "needIndex": false,
        "isHyperlink": true,
        ...colOverrides
      };
    });

  // Count occurrences of each column name
  const columnNameCounts = {};
  tableConfig.forEach((config) => {
    if (!config.skip) {
      const baseName = config.sqlColumn;
      columnNameCounts[baseName] = (columnNameCounts[baseName] || 0) + 1;
    }
  });

  // Add suffix to all columns that have duplicates
  const seenColumns = {};
  tableConfig.forEach((config) => {
    if (!config.skip) {
      const baseName = config.sqlColumn;
      if (columnNameCounts[baseName] > 1) {
        seenColumns[baseName] = (seenColumns[baseName] || 0) + 1;
        config.sqlColumn = `${baseName}_${seenColumns[baseName]}`;
      }
    }
  });

  return tableConfig;
}