import { auth, sheets_v4 } from "@googleapis/sheets";
import Sheets = sheets_v4.Sheets;

export type ColumnType = "string" | "number" | "boolean" | "date";
export type NullableColumnType = `${ColumnType}?` | ColumnType;
export type Table = {
  headerNames: string[];
  columnTypes: (NullableColumnType | undefined)[];
  data: any[][];
};

function isAllowedValue(value: any): boolean {
  return (
    value === undefined ||
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    value instanceof Date
  );
}

const isoDateRegex = /^\d{4}-\d{2}-\d{2}([T\s](\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[\+\-]\d{2}:\d{2})?)?)?$/;
function isISODate(str: string) {
  return isoDateRegex.test(str);
}

function tryParseDate(val: any): any {
  if (val === null || val === undefined || val instanceof Date) {
    return val;
  }
  const str = String(val);
  if (isISODate(str)) {
    return new Date(str);
  }
  return val;
}

export function toTable(
  rowsObject: any[],
  { emptyVal = null, parseDates = false }: { emptyVal?: any; parseDates?: boolean } = {}
): Table {
  const headerIndex: Record<string, number> = {};
  const headerNames: string[] = [];
  const rows: any[][] = [];
  for (let i = 0; i < rowsObject.length; i++) {
    const row = rowsObject[i];
    const rowArray = Array(headerNames.length).fill(emptyVal);
    if (typeof row !== "object") {
      throw new Error("Rows must be objects");
    }
    for (const [key, val] of Object.entries(row)) {
      if (!isAllowedValue(val)) {
        throw new Error(`Invalid value in row #${i}: ${typeof val} - ${val}. Full row: ${JSON.stringify(row)}`);
      }
      let idx = headerIndex[key];
      if (idx === undefined) {
        headerNames.push(key);
        idx = headerIndex[key] = headerNames.length - 1;
        rowArray.push(null);
        rows.forEach(r => r.push(emptyVal));
      }
      rowArray[idx] = parseDates ? tryParseDate(val) : val;
    }
    rows.push(rowArray);
  }

  const columnTypes = Array(headerNames.length).fill(undefined);
  for (let i = 0; i < headerNames.length; i++) {
    const values = rows.map(r => r[i]);
    const hasEmpty = values.some(v => v === emptyVal);
    if (values.every(v => typeof v === "string" || v === emptyVal)) {
      columnTypes[i] = "string";
    } else if (values.every(v => typeof v === "number" || v === emptyVal)) {
      columnTypes[i] = "number";
    } else if (values.every(v => typeof v === "boolean" || v === emptyVal)) {
      columnTypes[i] = "boolean";
    } else if (values.every(v => v instanceof Date || v === emptyVal)) {
      columnTypes[i] = "date";
    }
    if (columnTypes[i] && hasEmpty) {
      columnTypes[i] += "?";
    }
  }

  return {
    headerNames,
    columnTypes,
    data: rows,
  };
}

function formatDateForGoogleSpreadsheets(value: Date): string {
  const [date, timeOfDay] = value.toISOString().split("T");
  return `${date} ${timeOfDay.split(".")[0]}`;
}

export async function getSize(sheets: Sheets, spreadsheetId: string, sheetName): Promise<[number, number]> {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: sheetName,
  });

  const values = response.data.values || [];
  const rowCount = values.length;
  const columnCount = values.reduce((max, row) => Math.max(max, row.length), 0);
  return [rowCount, columnCount];
}

export async function saveToGoogleSheets(opts: {
  spreadsheetId: string;
  sheetName: string;
  credentials: {
    email: string;
    privateKey: string;
  };
  data: any[];
}) {
  const sheets = new sheets_v4.Sheets({
    auth: new auth.JWT({
      email: opts.credentials.email,
      key: opts.credentials.privateKey,
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    }),
  });
  const table = toTable(opts.data, { parseDates: true });
  // Clear the sheet first
  await sheets.spreadsheets.values.clear({
    spreadsheetId: opts.spreadsheetId,
    range: opts.sheetName,
  });

  // Append new data
  const rawValues = [table.headerNames, ...table.data];
  for (let i = 0; i < table.columnTypes.length; i++) {
    const type = table.columnTypes[i];
    if (type === "date" || type === "date?") {
      for (let k = 0; k < rawValues.length; k++) {
        const value = rawValues[k][i];
        if (value instanceof Date) {
          rawValues[k][i] = formatDateForGoogleSpreadsheets(value);
        }
      }
    }
  }
  console.log(`Raw values:\n${JSON.stringify(table, null, 2)}`);
  await sheets.spreadsheets.values.update({
    spreadsheetId: opts.spreadsheetId,
    range: opts.sheetName,
    valueInputOption: "RAW",
    requestBody: {
      values: rawValues,
    },
  });
}
