import { describe } from "node:test";
import { toTable } from "../src/google-sheets";
import { strictEqual, strict } from "assert";

describe("To Table Test", t => {
  const data = [
    { col1: "value1", col2: true, col4: "2024-10-30T20:11:51.321Z" },
    { col2: false, col1: 3 },
    { col2: false, col3: new Date(), col4: "2024-10-30T20:11:51.321Z" },
  ];
  const table = toTable(data, { parseDates: true });
  console.log(JSON.stringify(table, null, 2));

  strictEqual(table.headerNames.length, 4);
  strictEqual(table.data.length, 3);

  strictEqual(table.data[0][0], "value1");
  strictEqual(table.data[0][1], true);

  strict(table.data[2][2] instanceof Date, `Not a date: ${table.data[2][2]} - ${typeof table.data[2][2]}`);

  strictEqual(table.columnTypes[0], undefined);
  strictEqual(table.columnTypes[1], "boolean");
  strictEqual(table.columnTypes[2], "date?");
  strictEqual(table.columnTypes[3], "date?");
});

// describe("To Table Test 2", t => {
//
//   const data = [
//     {"name":"","email":"mayank@accorppartners.com","unsubscribed":null,"updated_at":"2024-10-30T15:45:28.000Z"},
//     {"name":"Reza Torbati","email":"reza@nation.dev","unsubscribed":null,"updated_at":"2024-10-30T14:25:07.000Z"},
//     {"name":"","email":"kaydashfedor@gmail.com","unsubscribed":null,"updated_at":"2024-10-30T13:46:39.000Z"},
//     {"name":"Hoai Ngo","email":"hoaint@bibabo.vn","unsubscribed":null,"updated_at":"2024-10-30T08:12:50.000Z"}
//   ]
//   const table = toTable(data);
//   console.log(JSON.stringify(table, null, 2));
// })
