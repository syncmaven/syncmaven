import { test, expect } from "bun:test";
import { maskPassword, splitName } from "../src/lib/util";

test("maskPassword", () => {
  const password = "pAssWord";
  const masked = maskPassword(`postgresql://user:${password}@pg.company.com/database?sslmode=no-verify&schema=data`);
  const expected = `postgresql://user:****@pg.company.com/database?sslmode=no-verify&schema=data`;
  console.log(masked);
  expect(masked).toBe(expected);
});

test("splitName", () => {
  const test1 = splitName("John Doe");
  const test2 = splitName("Jack");
  expect(test1).toEqual({ first: "John", last: "Doe" });
  expect(test2).toEqual({ first: "Jack", last: "" });
});
