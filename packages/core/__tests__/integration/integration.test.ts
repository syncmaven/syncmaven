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
let postgresConnectionString: string = "";

type PostgresContainer = {
  shutdown: () => Promise<void>;
  connectionString: string;
};

describe("integration tests", c => {
  after(async () => {
    if (container) {
      await container.stop();
    }
  });
  test("test-postgres", async t => {
    const container = await startPostgresContainer();
    let client: Client | undefined = undefined;

    try {
      client = new Client({
        connectionString: postgresConnectionString,
      });
      await client.connect();
      console.log(`Connected to database.`);

      await t.test("date cursor", async t => {
        await runTest(
          "postgres",
          "date_cursor",
          {
            name: "test_postgres_date_cursor",
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
            name: "test_postgres_int_cursor",
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
            name: "test_snowflake_date_cursor",
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
            name: "test_snowflake_int_cursor",
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
            name: "test_bigquery_date_cursor",
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
            name: "test_bigquery_int_cursor",
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

async function startPostgresContainer(): Promise<PostgresContainer> {
  const image = "postgres:latest";
  try {
    const pullStream = await docker.pull(image);
    await new Promise(res => docker.modem.followProgress(pullStream, res));
    console.log(`Image ${image} pulled. Creating container...`);
  } catch (e) {
    console.error(`Failed to pull image ${image} Trying with local one.`, { cause: e });
  }

  container = await docker.createContainer({
    Image: "postgres:latest",
    Env: ["POSTGRES_USER=test", "POSTGRES_PASSWORD=test", "POSTGRES_DB=test"],
    HostConfig: {
      AutoRemove: true,
      ExtraHosts: ["host.docker.internal:host-gateway"],
      PortBindings: { "5432/tcp": [{}] },
    },
    Healthcheck: {
      Test: ["CMD-SHELL", "pg_isready -U test"],
      Interval: 1000000 * 1000, // 1s
      Timeout: 1000000 * 1000, // 1s
      Retries: 10,
      StartPeriod: 1000000 * 1000, // 1s
    },
    AttachStdin: true,
    AttachStdout: true,
    AttachStderr: true,
    OpenStdin: true,
    StdinOnce: false,
    Tty: false,
    name: `syncmaven-test-postgres`,
  });

  await container.start();
  let healthy = false;
  for (let i = 0; i < 10; i++) {
    const stats = await container.inspect();
    if (stats.State.Running) {
      if (stats.State.Health?.Status === "healthy") {
        healthy = true;
        const p = stats.NetworkSettings.Ports["5432/tcp"];
        postgresPort = parseInt(p[0].HostPort, 10);
        console.log(`Using port ${postgresPort} for postgres.`);
        break;
      }
    }
    await new Promise(res => setTimeout(res, 1000));
  }
  if (!healthy) {
    throw new Error("Postgres container is not healthy.");
  }

  postgresConnectionString = `postgres://test:test@localhost:${postgresPort}/test`;

  console.log(`Postgres container started on port ${postgresPort}. Id: ${container.id}`);
  return {
    shutdown: async () => {
      if (container) {
        try {
          await container.stop();
        } catch (e) {
          console.error("Failed to stop container:", e);
        }
        console.log("Container stopped and removed.");
      }
    },
    connectionString: postgresConnectionString,
  };
}
