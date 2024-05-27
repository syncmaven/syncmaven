import Prism, { Grammar } from "prismjs";
import { fmt } from "../log";

export type ColorScheme = Record<string, keyof typeof fmt | undefined>;

export const defaultColorScheme: ColorScheme = {
  punctuation: "gray",
  "string-property": "violet",
  string: "green",
  keyword: "bold",
  "function-variable": undefined,
};

function chalkString(expr: keyof typeof fmt, str: string): string {
  return fmt[expr](str);
}

function getGrammar(lang: "json") {
  switch (lang) {
    case "json":
      return Prism.languages.js;
    default:
      throw new Error(`Unsupported language ${lang}`);
  }
}

/**
 * Highlights code with Prism and the applies Chalk to color
 * output for terminal
 */
export function codeHighlight(code: string, lang: "json", colorScheme: ColorScheme = defaultColorScheme): string {
  return Prism.tokenize(code, getGrammar(lang))
    .map(element => {
      if (typeof element === "string") {
        return element;
      } else {
        let highlight = colorScheme[element.type];
        return highlight ? chalkString(highlight, element.content.toString()) : `${element.content}`;
      }
    })
    .join("");
}
