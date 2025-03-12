import axios from 'axios';

function formatSummary(data) {
  const {
    inputFile,
    tableName,
    totalRows,
    validRows,
    emptyRows,
    skippedRows,
    duration,
    sheetName
  } = data;

  return [
    "📊 *Import Summary*",
    `• File: \`${inputFile}\`${sheetName ? ` (Sheet: ${sheetName})` : ''}`,
    `• Table: \`${tableName}\``,
    `• Total Rows: ${totalRows}`,
    `• Successfully Imported: ${validRows}`,
    `• Empty Rows Skipped: ${emptyRows}`,
    `• Invalid Rows Skipped: ${skippedRows}`,
    `• Processing Time: ${duration}s`
  ].join('\n');
}

export async function sendSlackNotification(webhookUrl, message, summaryData = null) {
  if (!webhookUrl) return;

  try {
    const payload = {
      text: message,
      ...(summaryData && {
        attachments: [{
          color: summaryData.validRows > 0 ? 'good' : 'danger',
          text: formatSummary(summaryData)
        }]
      })
    };

    await axios.post(webhookUrl, payload);
  } catch (error) {
    console.error('Failed to send Slack notification:', error.message);
  }
} 