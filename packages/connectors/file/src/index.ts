import { z } from "zod";
import { BaseOutputStream, DestinationProvider, OutputStreamConfiguration, stdProtocol } from "@syncmaven/node-cdk";
import { ExecutionContext } from "@syncmaven/protocol";
import * as fs from "node:fs";

export const FileCredentials = z.object({
  filePath: z.string().describe("Absolute path of file where to write data"),
});

export type FileCredentials = z.infer<typeof FileCredentials>;

const FileRow = z.object({}).passthrough();

type FileRow = z.infer<typeof FileRow>;

class FileStream extends BaseOutputStream<FileRow, FileCredentials> {
  private file: fs.WriteStream;

  constructor(config: OutputStreamConfiguration<FileCredentials>, ctx: ExecutionContext) {
    super(config, ctx);
    this.file = fs.createWriteStream(config.credentials.filePath);
  }

  async init() {
    return this;
  }

  public async handleRow(row: FileRow, ctx: ExecutionContext) {
    this.file.write(JSON.stringify(row) + "\n");
  }
}

export const fileProvider: DestinationProvider<FileCredentials> = {
  name: "file",
  credentialsType: FileCredentials,
  streams: [
    {
      name: "raw",
      rowType: FileRow,
      createOutputStream: (config, ctx) => new FileStream(config, ctx).init(),
    },
  ],
  defaultStream: "raw",
};

stdProtocol(fileProvider);
