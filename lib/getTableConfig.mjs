import XLSX from 'xlsx';
import fs from 'fs';

export function getTableConfigOverrides(tableConfigFile) {
    if (tableConfigFile && fs.existsSync(tableConfigFile)) {
        const tableConfig = JSON.parse(fs.readFileSync(tableConfigFile, 'utf-8'));
        return tableConfig
    }
    return {}
}

export function getTableConfigForAWorkSheet(worksheet, tableConfigFile) {
    const tableConfigOverrides = getTableConfigOverrides(tableConfigFile);
    const range = XLSX.utils.decode_range(worksheet['!ref'] || 'A1');

    // First get all headers and create base configs
    const tableConfig = Array(range.e.c + 1).fill(1).map((_, idx) => {
        const cellAddress = XLSX.utils.encode_cell({ r: 0, c: idx }); // headers
        const cell = worksheet[cellAddress];
        const colVal = cell?.v || `Column${idx + 1}`;
        const colOverrides = tableConfigOverrides?.[colVal] ?? {};
        
        return {
            "header": colVal,
            "sqlColumn": colVal,
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