/**
 * Mask the password in the url so it could be logged securely
 * @param url url
 */
export function maskPassword(url: string): string {
  let urlObj: URL;
  try {
    urlObj = new URL(url);
  } catch (e) {
    //mask the whole url if it's not a valid url
    return "******";
  }
  urlObj = new URL(url);
    const password = urlObj.password;
    if (password) {
        urlObj.password = "****";
    }
    return urlObj.toString();
}

export function isTruish(x: any): boolean {
  return x === "true" || x === "1";
}

export function splitName(name: string): {first: string, last: string} {
  const [first, ...rest] = name.split(" ");
  return {
    first,
    last: rest.join(" "),
  };
}
