import { describe, test, before, after } from "node:test";

import { Client } from "pg";
import assert from "assert";

import { runTest } from "./lib";
import { connectSnowflake, SnowflakeCredentials, snowflakeQuery } from "../../src/datasources/snowlake";
import snowflake from "snowflake-sdk";
import { BigQuery } from "@google-cloud/bigquery";
import { BigQueryCredentials, bqQuery } from "../../src/datasources/bigquery";
import Docker from "dockerode";
import * as net from "node:net";

const docker = new Docker();
let container: Docker.Container | null = null;
let postgresPort: number = 5432;
let postgresConnectionString: string = ""

type PostgresContainer = {
  shutdown: () => Promise<void>;
  connectionString: string;
}

async function startPostgresContainer(): Promise<PostgresContainer> {
  postgresPort = await findAvailablePort(5432);
  console.log(`Using port ${postgresPort} for postgres.`);

  container = await docker.createContainer({
    Image: 'postgres:latest',
    Env: ['POSTGRES_USER=test', 'POSTGRES_PASSWORD=test', 'POSTGRES_DB=testdb'],
    HostConfig: {
      AutoRemove: true,
      AttachStdin: true,
      AttachStdout: true,
      AttachStderr: true,
      OpenStdin: true,
      StdinOnce: false,
      ExtraHosts: ["host.docker.internal:host-gateway"],
      Tty: false,
      name: `syncmaven-test-postgres-${postgresPort}`,
      PortBindings: { '5432/tcp': [{ HostPort: postgresPort.toString() }] },
    },
  });
  postgresConnectionString = `postgres://test:test@localhost:${postgresPort}/testdb`;

  await container.start();
  console.log(`Postgres container started on port ${postgresPort}. Id: ${container.id}`);
  return {
    shutdown: async () => {
      if (container) {
        try {
          await container.stop();
        } catch (e) {
          console.error('Failed to stop container:', e);
        }
        console.log('Container stopped and removed.');
      }
    },
    connectionString: postgresConnectionString,
  }
}

async function findAvailablePort(startPort: number): Promise<number> {
  let port = startPort;

  while (true) {
    const isAvailable = await new Promise((resolve) => {
      const server = net.createServer();
      server.once('error', () => resolve(false));
      server.once('listening', () => {
        server.close(() => resolve(true));
      });
      server.listen(port);
    });

    if (isAvailable) {
      return port;
    }
    port += 1;
  }
}
describe("integration tests", c => {
  test("test-postgres", async t => {
    const container = await startPostgresContainer();
    let client: Client | undefined = undefined;

    try {
      client = new Client({
        connectionString: postgresConnectionString,
      });
      await client.connect();

      await t.test("date cursor", async t => {
        await runTest(
          "postgres",
          "date_cursor",
          {
            name: "test_date_cursor",
            cursor: "time",
            datasource: postgresConnectionString,
            query:
              "select * from syncmaven_test.syncmaven_test_table where :cursor is null or time >= :cursor order by id asc",
          },
          async query => {
            await client!.query(query);
          }
        );
      });

      await t.test("int cursor", async t => {
        await runTest(
          "postgres",
          "int_cursor",
          {
            name: "test_int_cursor",
            cursor: "id",
            datasource: postgresConnectionString,
            query:
              "select * from syncmaven_test.syncmaven_test_table where :cursor is null or id >= :cursor order by id asc",
          },
          async query => {
            await client!.query(query);
          }
        );
      });
    } catch (e: any) {
      assert.fail(e);
    } finally {
      if (client) {
        await client.end();
        console.log(`Disconnected from database.`);
      }
      await container.shutdown();
    }
  });

  test("test-snowflake", async t => {
    if (!process.env.TEST_SNOWFLAKE) {
      t.skip("TEST_SNOWFLAKE is not set.");
      return;
    }
    let connection: snowflake.Connection | undefined = undefined;

    try {
      const credJson = process.env.TEST_SNOWFLAKE;
      const cred = SnowflakeCredentials.parse(JSON.parse(credJson));
      connection = await connectSnowflake(cred);

      await t.test("date cursor", async t => {
        await runTest(
          "snowflake",
          "date_cursor",
          {
            name: "test_date_cursor",
            cursor: "time",
            datasource: {
              type: "snowflake",
              credentials: cred,
            },
            query: "select * from SYNCMAVEN_TEST_TABLE where :cursor is null or TIME >= :cursor order by ID asc",
          },
          async query => {
            await snowflakeQuery(connection!, query);
          }
        );
      });

      await t.test("int cursor", async t => {
        await runTest(
          "snowflake",
          "int_cursor",
          {
            name: "test_int_cursor",
            cursor: "id",
            datasource: {
              type: "snowflake",
              credentials: cred,
            },
            query: "select * from SYNCMAVEN_TEST_TABLE where :cursor is null or ID >= :cursor order by ID asc",
          },
          async query => {
            await snowflakeQuery(connection!, query);
          }
        );
      });
    } catch (e: any) {
      assert.fail(e);
    } finally {
      if (connection) {
        connection.destroy(() => {});
        console.log(`Disconnected from database.`);
      }
    }
  });

  test("test-bigquery", async t => {
    if (!process.env.TEST_BIGQUERY) {
      t.skip("TEST_BIGQUERY is not set.");
      return;
    }
    let bigQuery: BigQuery | undefined = undefined;

    try {
      const credJson = process.env.TEST_BIGQUERY;
      const cred = BigQueryCredentials.parse(JSON.parse(credJson));
      bigQuery = new BigQuery({
        credentials: typeof cred.key === "string" ? JSON.parse(cred.key) : cred.key,
        projectId: cred.projectId,
      });

      await t.test("date cursor", async t => {
        await runTest(
          "bigquery",
          "date_cursor",
          {
            name: "test_date_cursor",
            cursor: "time",
            datasource: {
              type: "bigquery",
              credentials: cred,
            },
            query:
              "select * from syncmaven_test.syncmaven_test_table s where @cursor is null or s.time >= IFNULL(@cursor, s.time) order by s.id asc",
          },
          async query => {
            await bqQuery(bigQuery!, query);
          }
        );
      });

      await t.test("int cursor", async t => {
        await runTest(
          "bigquery",
          "int_cursor",
          {
            name: "test_int_cursor",
            cursor: "id",
            datasource: {
              type: "bigquery",
              credentials: cred,
            },
            query:
              "select * from syncmaven_test.syncmaven_test_table s where @cursor is null or s.id >= IFNULL(@cursor, s.id) order by s.id asc",
          },
          async query => {
            await bqQuery(bigQuery!, query);
          }
        );
      });
    } catch (e: any) {
      assert.fail(e);
    } finally {
    }
  });
});
