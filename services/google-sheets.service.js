// services/google-sheets.service.js
// Appends rows to a Google Sheet via a service account JWT.

import { google } from 'googleapis';
import { GOOGLE_PRIVATE_KEY, GOOGLE_CLIENT_EMAIL, SPREADSHEET_ID, SHEET_NAME } from '../config.js';
import { formatToEasternTime } from '../utils/time.utils.js';

function getAuth() {
  return new google.auth.JWT({
    email: GOOGLE_CLIENT_EMAIL,
    key: GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
}

async function appendRows(rows) {
  const sheets = google.sheets({ version: 'v4', auth: getAuth() });
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A:Z`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: rows },
  });
}

export async function logAlertToSheet(
  alertType, driverName, vehicleNumber,
  formattedDate, severity, speedRange,
  forwardVideoUrl, inwardVideoUrl
) {
  const ts = formatToEasternTime(new Date().toISOString());
  await appendRows([[
    alertType, driverName, vehicleNumber,
    formattedDate, severity, speedRange,
    forwardVideoUrl, inwardVideoUrl, ts,
  ]]);
}