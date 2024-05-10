import { test } from "node:test";
import assert from "assert";
import { maskPassword, splitName } from "../src/lib/util";

test("maskPassword", () => {
  const password = "pAssWord";
  const masked = maskPassword(`postgresql://user:${password}@pg.company.com/database?sslmode=no-verify&schema=data`);
  const expected = `postgresql://user:****@pg.company.com/database?sslmode=no-verify&schema=data`;
  console.log(masked);
  assert.strictEqual(masked, expected);
});

test("splitName", () => {
  const test1 = splitName("John Doe");
  const test2 = splitName("Jack");
  assert.deepEqual(test1, { first: "John", last: "Doe" });
  assert.deepEqual(test2, { first: "Jack", last: "" });
});
