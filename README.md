# import_to_psql

A command-line tool to transfer data from Excel (XLSX/XLS) or CSV files to PostgreSQL tables with support for data transformation, batch processing, and progress tracking.

## Features

- 📊 Support for both Excel (XLSX/XLS) and CSV files
- 🔄 Configurable data transformations
- 📅 Timezone-aware date parsing
- 🚀 Batch processing for better performance
- 📈 Progress bars and status updates
- 🔔 Slack notifications
- 🛠️ Configurable column mappings
- 🔒 Safe table updates with transaction support

## Prerequisites

- Node.js >= 16.0.0
- PostgreSQL database
- Environment variables setup (see Configuration section)

## Installation

```bash
npm install -g import_to_psql
```

## Configuration

1. Create a `.env` file with your database connection:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
```

2. Create a table configuration JSON file (e.g., `config.json`):

```json
[
{
"header": "Excel Column Name",
"sqlColumn": "db_column_name",
"fieldType": "string|number|timestamp",
"primary": false,
"notNull": false
}
]
```

## Usage

```bash
import_to_psql \
--input-file data.xlsx \
--table schema.table_name \
--table-config config.json \
--timezone Asia/Kolkata \
--batch-size 5000 \
--truncate false \
--sheet-name "Sheet1" \
--slack-notify-url "https://hooks.slack.com/services/xxx"
```

## Configuration

### Options

- `--input-file` (required): Path to input Excel or CSV file
- `--table` (required): Target PostgreSQL table (format: schema.table)
- `--table-config` (required): JSON file containing table configuration
- `--timezone` (required): Timezone for date parsing (e.g., Asia/Kolkata)
- `--batch-size` (optional): Number of records per batch insert (default: 5000)
- `--truncate` (optional): Truncate table before import (default: false)
- `--sheet-name` (optional): Excel sheet name (defaults to first sheet)
- `--slack-notify-url` (optional): Slack webhook URL for notifications

## Field Types

The tool supports the following field types in the configuration:

- `string`: Text data (maps to PostgreSQL TEXT)
- `number`: Numeric data (maps to PostgreSQL NUMERIC)
- `timestamp`: Date/time data (maps to PostgreSQL TIMESTAMP)

## Error Handling

- Invalid data rows are skipped and logged
- Progress is displayed in real-time
- Detailed error messages and summaries
- Transaction rollback on errors
- Slack notifications for success/failure

## Development

```bash
# Run tests
npm test

# Run tests with coverage
npm run test:coverage
```

## Author

Kishore Renangi (kishorer@knownuggets.com)

## License

MIT

