import { CommonOpts, ProjectDirOpt } from "./index";
import { out, rewriteSeverityLevel } from "../log";
import path from "path";
import { configureEnvVars, readProject, untildify } from "../lib/project";
import assert from "assert";
import { createDatasource } from "../datasources";
import { Table } from "console-table-printer";

export async function preview(modelName: string, opts: ProjectDirOpt & CommonOpts) {
  const projectDir = path.resolve(untildify(opts?.projectDir || process.env.SYNCMAVEN_PROJECT_DIR || process.cwd()));
  configureEnvVars([projectDir, "."], opts.env || []);
  rewriteSeverityLevel("INFO", "DEBUG");
  console.log(`Previewing ${modelName} in ${opts.projectDir}`);
  const project = readProject(projectDir);
  const modelFactory = project.models[modelName];
  assert(modelFactory, `Model ${modelName} not found in ${projectDir}`);
  const model = modelFactory();
  const datasource = await createDatasource(model);
  const query = model.query;
  const maxRows = 10;
  let rows = 0;
  let p: Table | undefined = undefined;
  let hasMore = false;
  await datasource.executeQuery({
    query,
    handler: {
      header: header => {
        p = new Table({
          columns: header.columns.map(c => ({ name: c.name, alignment: "left" })),
        });
      },
      row: row => {
        assert(p, "Table not initialized. header() is not called before row()");
        rows++;
        if (rows < maxRows) {
          p.addRow(row);
        } else {
          hasMore = true;
          return true;
        }
      },
    },
  });
  out(p!.render());
  if (hasMore) {
    out(`...and more. The result set is truncated to ${maxRows} rows.`);
  }
}
