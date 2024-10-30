import { z } from "zod";
import {
  BaseOutputStream,
  CredentialsCanBeEmpty,
  RateLimitError,
  DestinationProvider,
  normalizeEmail,
  OutputStreamConfiguration,
  splitName,
  stdProtocol,
} from "@syncmaven/node-cdk";
import { ExecutionContext } from "@syncmaven/protocol";
import assert from "assert";

import JSON5 from "json5";
import { saveToGoogleSheets } from "./google-sheets";

export const GoogleSheetsCredentials = z.object({
  key: z.any(),
});

export type GoogleSheetsCredentials = z.infer<typeof GoogleSheetsCredentials>;

const GoogleSheetsRow = z.any();

type GoogleSheetsRow = z.infer<typeof GoogleSheetsRow>;

class GoogleSheetsOutputStream extends BaseOutputStream<
  GoogleSheetsRow,
  CredentialsCanBeEmpty<GoogleSheetsCredentials>
> {
  private rows: any[] = [];
  private connection: GoogleSheetsCredentials;
  private streamOptions: GoogleSheetsStreamOptions;

  constructor(
    config: OutputStreamConfiguration<GoogleSheetsCredentials, GoogleSheetsStreamOptions>,
    ctx: ExecutionContext
  ) {
    super(config, ctx);
    this.connection = config.credentials;
    this.streamOptions = config.options;
    console.debug(`streamOptions: ${JSON.stringify(this.streamOptions)}`);
  }

  async init(ctx: ExecutionContext) {
    return this;
  }

  async handleRow(row: GoogleSheetsRow, ctx: ExecutionContext) {
    this.rows.push(row);
  }

  async finish(ctx: ExecutionContext) {
    console.log(`Flushing ${this.rows.length} rows to Google Sheets`);
    const key = typeof this.connection.key === "string" ? JSON5.parse(this.connection.key) : this.connection.key;
    assert(key.client_email, "client_email is required in Google Sheets key");
    assert(key.private_key, "private_key is required in Google Sheets key");
    await saveToGoogleSheets({
      spreadsheetId: this.streamOptions.spreadsheetId,
      sheetName: this.streamOptions.sheetName,
      credentials: {
        email: key.client_email,
        privateKey: key.private_key,
      },
      data: this.rows,
    });
  }
}

export const GoogleSheetsStreamOptions = z.object({
  spreadsheetId: z.string(),
  sheetName: z.string(),
});

export type GoogleSheetsStreamOptions = z.infer<typeof GoogleSheetsStreamOptions>;

export const googleSheetsProvider: DestinationProvider<GoogleSheetsCredentials> = {
  name: "google-sheets",
  credentialsType: GoogleSheetsCredentials,
  streams: [
    {
      name: "sheet",
      streamOptions: GoogleSheetsStreamOptions,
      rowType: GoogleSheetsRow,
      createOutputStream: (config, ctx) => new GoogleSheetsOutputStream(config, ctx).init(ctx),
    },
  ],
  defaultStream: "sheet",
};

stdProtocol(googleSheetsProvider);
