import fs from "fs";
import path from "path";
import { ModelDefinition } from "../../src/types/objects";
import { connectorDev } from "../../src/commands/connector-dev";
import assert from "assert";
import os from "node:os";

export async function runTest(
  type: string,
  name: string,
  model: ModelDefinition,
  queryCb: (query: string) => Promise<void>
) {
  let tmpDir = "";
  try {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), `syncmaven-test-${type}-${name}`));
    console.log(`Created temporary directory: ${tmpDir}`);

    await processSqlFile(`__tests__/test-data/${type}/part1.sql`, queryCb);

    await runSync(tmpDir, model, `__tests__/test-data/${type}/expected_part1_${name}.ndjson`);

    await processSqlFile(`__tests__/test-data/${type}/part2.sql`, queryCb);

    await runSync(tmpDir, model, `__tests__/test-data/${type}/expected_part2_${name}.ndjson`);
  } finally {
    if (tmpDir) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
      console.log(`Deleted temporary directory: ${tmpDir}`);
    }
    try {
      await processSqlFile(`__tests__/test-data/${type}/destroy.sql`, queryCb);
    } catch (e: any) {
      console.error(`Error clearing database: ${e}`);
    }
  }
}

export async function runSync(projectDir: string, model: ModelDefinition, expectedFile: string) {
  const modelsPath = path.join(projectDir, "models");
  fs.mkdirSync(modelsPath, { recursive: true });
  const modelPath = path.join(modelsPath, "model.json");
  fs.writeFileSync(modelPath, JSON.stringify(model, null, 2));

  const connectionsPath = path.join(projectDir, "connections");
  fs.mkdirSync(connectionsPath, { recursive: true });
  const connectionPath = path.join(connectionsPath, "connection.json");
  const resultPath = path.join(projectDir, "result.ndjson");

  const fileConnectorDir = path.join(process.cwd(), "../connectors/file");
  console.log(`File connector dir: ${fileConnectorDir}`);
  fs.writeFileSync(
    connectionPath,
    JSON.stringify(
      {
        package: {
          type: "npm",
          dir: fileConnectorDir,
        },
        credentials: {
          filePath: resultPath,
        },
      },
      null,
      2
    )
  );

  await connectorDev(fileConnectorDir, {
    modelFile: modelPath,
    sync: "sync",
    connectionFile: connectionPath,
    state: projectDir,
  });

  //compare file on resultPath with expectedFile
  const result = fs.readFileSync(resultPath, "utf8");
  const expected = fs.readFileSync(expectedFile, "utf8");

  assert.equal(result, expected, "Result file does not match expected file");
}

export async function processSqlFile(filePath: string, cb: (query: string) => Promise<void>) {
  try {
    // Read the SQL file
    const sql = fs.readFileSync(filePath, "utf8");

    // Split the file content into individual queries
    const queries = sql
      .split(";")
      .map(query => query.trim())
      .filter(query => query.length > 0); // Remove empty queries

    // Execute each query sequentially
    for (const query of queries) {
      console.log(`Executing query: ${query}`);
      await cb(query);
    }

    console.log("All queries executed successfully");
  } catch (err) {
    throw new Error(`Error processing SQL file: ${err}`);
  }
}
