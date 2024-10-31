import Handlebars from "handlebars";

export type TemlateContext = Record<string, any>;
export type Template<T> = (vars: TemlateContext) => T;
type ReplacerCallback = (varName: string, defaultVal?: string) => string;

/**
 * Used in conjunction with replaceExpressions to replace ${env.NAME} legacy env syntax
 * @param opts.varName
 * @param opts.defaultVal
 */
function createEnvVariablesReplacer(
  opts: { filePath?: string; env?: Record<string, string | undefined> } = {}
): ReplacerCallback {
  return (varName: string, defaultVal?: string) => {
    const [prefix, ...rest] = varName.split(".");
    if (prefix === "env") {
      const value = (opts.env || process.env)[rest.join(".")];
      if (value === undefined) {
        if (defaultVal === undefined) {
          throw new Error(
            `Environment variable ${varName} is not set. It's used in ${opts.filePath || "unknown file"}`
          );
        } else {
          return defaultVal;
        }
      }
      return value;
    } else {
      throw new Error(
        `Unsupported placeholder \${${varName}} in ${opts.filePath || "unknown file"}. Only \${env.NAME} placeholders are supported. Did you mean \${env.${varName}}?`
      );
    }
  };
}

export interface TemplateEngine {
  /**
   * Compiles the obj with the given variables
   * @param obj can be either string or object, in the latter case it will be traversed recursively
   * @param vars variables to replace in the template
   * @returns the function that can be called with the variables to get the final result
   */
  compile: <T>(obj: T, location: { fileName?: string }) => Template<T>;
}

export abstract class GenericTemplateEngine implements TemplateEngine {
  compile<T>(obj: T, location): Template<T> {
    const compiled = traverseObject(obj, value => {
      if (typeof value === "string") {
        return this.compileString(value, location);
      }
      return value;
    });

    return (vars: TemlateContext) => {
      return traverseObject(compiled, value => {
        if (typeof value === "function") {
          return value(vars);
        }
        return value;
      });
    };
  }

  protected abstract compileString(template: string, location: { fileName?: string }): Template<string>;
}

export function replaceExpressions(value: string, callback: ReplacerCallback): any {
  const regex = /\$\{([a-zA-Z0-9_.-]+)(?::([^}]*))?\}/g;
  return value.replace(regex, (match, varName, defaultVal) => callback(varName.trim(), defaultVal?.trim()));
}

export class HandlebarsTemplateEngine extends GenericTemplateEngine {
  constructor() {
    super();
  }

  protected compileString(template: string, location): Template<string> {
    const compiled = Handlebars.compile(template);
    return (vars: TemlateContext) => {
      return replaceExpressions(
        compiled(vars),
        createEnvVariablesReplacer({ filePath: location.fileName, env: vars.env || process.env })
      );
    };
  }
}

function isLeaf(node: any) {
  return node === null || node === undefined || typeof node !== "object";
}

/**
 * Traverses the object and calls the callback on each leaf node. Leaf nodes that
 * doesn't have any properties - numbers, strings, booleans, null, undefined.
 *
 * On those nodes the callback is called with the value and the return value is
 * @param obj
 * @param callback
 */
function traverseObject<T>(obj: T, callback: (value: any) => any): T {
  const traverse = (node: any): any => {
    if (isLeaf(node)) {
      return callback(node);
    }

    if (Array.isArray(node)) {
      return node.map(item => traverse(item));
    }

    const result: any = {};
    for (const key in node) {
      if (node.hasOwnProperty(key)) {
        result[key] = traverse(node[key]);
      }
    }
    return result;
  };

  return traverse(obj);
}
