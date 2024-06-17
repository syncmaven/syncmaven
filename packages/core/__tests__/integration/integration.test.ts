import { describe, test } from "node:test";

import { Client } from "pg";
import assert from "assert";

import { runTest } from "./lib";
import { connectSnowflake, SnowflakeCredentials, snowflakeQuery } from "../../src/datasources/snowlake";
import snowflake from "snowflake-sdk";
import { BigQuery } from "@google-cloud/bigquery";
import { BigQueryCredentials, bqQuery } from "../../src/datasources/bigquery";

describe("integration tests", c => {
  test("test-postgres", async t => {
    if (!process.env.TEST_POSTGRES) {
      t.skip("TEST_POSTGRES is not set.");
      return;
    }
    let client: Client | undefined = undefined;

    try {
      const dsn = process.env.TEST_POSTGRES;
      client = new Client({
        connectionString: dsn,
      });
      await client.connect();

      await t.test("date cursor", async t => {
        await runTest(
          "postgres",
          "date_cursor",
          {
            name: "test_date_cursor",
            cursor: "time",
            datasource: dsn,
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
            datasource: dsn,
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
