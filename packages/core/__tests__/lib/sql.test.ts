import test from "node:test";
import { SqlQuery } from "../../src/lib/sql";
import assert from "assert";
import { genericToQueryParameter, SQLValue } from "../../src/datasources";

test("test-sql-query-parser", () => {
  const query = new SqlQuery(
    'select * from bi."users-segmented" where :cursor is null or updated_at >= :cursor',
    "postgres",
    (paramVal: SQLValue) => {
      return genericToQueryParameter(paramVal, "TIMESTAMP WITH TIME ZONE");
    }
  );
  assert.deepEqual(query.getNamedParameters(), ["cursor"]);
  assert.equal(
    query.compile({ cursor: new Date("2024-05-23T17:49:21.413Z") }),
    "SELECT * FROM \"bi\".\"users-segmented\" WHERE CAST('2024-05-23T17:49:21.413Z' AS TIMESTAMP WITH TIME ZONE) IS NULL OR updated_at >= CAST('2024-05-23T17:49:21.413Z' AS TIMESTAMP WITH TIME ZONE)"
  );
  assert.equal(
    query.compile({ cursor: null }),
    'SELECT * FROM "bi"."users-segmented" WHERE NULL IS NULL OR updated_at >= NULL'
  );
});
