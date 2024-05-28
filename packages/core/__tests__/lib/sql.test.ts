import test from "node:test";
import { SqlQuery } from "../../src/lib/sql";
import assert from "assert";

test("test-sql-query-parser", () => {
  const query = new SqlQuery(
    'select * from bi."users-segmented" where :cursor is null or updated_at >= :cursor',
    "postgres"
  );
  assert.deepEqual(query.getUsedNamedParameters(), ["cursor"]);
  assert.equal(
    query.compile({ cursor: new Date("2024-05-23T17:49:21.413Z") }),
    "SELECT * FROM \"bi\".\"users-segmented\" WHERE '2024-05-23T17:49:21.413Z'::TIMESTAMP WITH TIMEZONE IS NULL OR updated_at >= '2024-05-23T17:49:21.413Z'::TIMESTAMP WITH TIMEZONE"
  );
  assert.equal(
    query.compile({ cursor: null }),
    'SELECT * FROM "bi"."users-segmented" WHERE NULL IS NULL OR updated_at >= NULL'
  );
});
