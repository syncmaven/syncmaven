import { test } from "node:test";
import { initializeConsoleLogging } from "../../src/log";
import assert from "assert";
import { HandlebarsTemplateEngine, TemlateContext } from "../../src/lib/template";

initializeConsoleLogging();

function run<T>(obj: T, ctx: TemlateContext = {}): T {
  return new HandlebarsTemplateEngine().compile(obj, { fileName: "test" })({
    env: process.env,
    ...ctx,
  });
}

test("test-string-replace", () => {
  const newSyntax = run("{{ env.PWD }} + {{ env.NON_EXISTING_VARIALBE }}");
  console.log(newSyntax);
  assert.strictEqual(newSyntax, `${process.env.PWD} + `);
});

test("test-legacy-string-replace", () => {
  const legacySyntax = run("${env.PWD} + ${env.NON_EXISTING_VARIALBE:default}");
  console.log(legacySyntax);
  assert.strictEqual(legacySyntax, `${process.env.PWD} + default`);
});

test("object-replace", () => {
  const configCallArgs: any[][] = [];
  const result = run(
    {
      a: "${env.PWD} + ${env.NON_EXISTING_VARIALBE:default}",
      b: {
        c: "{{ env.PWD }} + {{ env.NON_EXISTING_VARIALBE }}",
      },
      e: '{{ config "first" env.PWD }}',
      numberNode: 1,
      nullNode: null,
      undefinedNode: undefined,
      booleanNode: true,
      stringNode: "string",
    },
    {
      config: (...args: any[]) => {
        configCallArgs.push(args.slice(0, args.length - 1));
        return "config";
      },
    }
  );
  console.log(`Template result: ${JSON.stringify(result, null, 2)}`);
  console.log(`Config call stack: ${JSON.stringify(configCallArgs, null, 2)}`);
  assert.deepEqual(result, {
    a: `${process.env.PWD} + default`,
    b: {
      c: `${process.env.PWD} + `,
    },
    e: "config",
    numberNode: 1,
    nullNode: null,
    booleanNode: true,
    undefinedNode: undefined,
    stringNode: "string",
  });
  assert.deepEqual(configCallArgs, [["first", process.env.PWD]]);
});
