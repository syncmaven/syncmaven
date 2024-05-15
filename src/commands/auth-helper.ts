import assert from "assert";
import { OAuth2Client } from "google-auth-library";
import { readProject } from "../lib/project";
import { waitForRequest } from "../lib/cli-http";

export const defaultOauthRedirectURIPort = 4512;

export async function triggerOauthFlow(projectDir: string, opts: { projectDir?: string; connection: string; port: string }) {
  const port = opts.port ? parseInt(opts.port) : defaultOauthRedirectURIPort;
  projectDir = projectDir || opts.projectDir || process.cwd();
  const project = readProject(projectDir);
  const connectionFactory = project.connection[opts.connection];
  assert(connectionFactory, `Connection with id ${opts.connection} not found in the project`);
  const connection = connectionFactory();
  const redirectUrl = `http://localhost:${port}`;
  if (connection.kind === "google-ads") {
    const oauth2Client = new OAuth2Client(
      connection.credentials.clientId,
      connection.credentials.clientSecret,
      redirectUrl
    );
    const url = oauth2Client.generateAuthUrl({
      access_type: "offline",
      scope: ["https://www.googleapis.com/auth/adwords"],
      prompt: "consent",
    });
    console.debug(
      `Make sure you have set up the redirect URI in the Google Cloud Console. Redirect URL: ${redirectUrl}`
    );
    console.log(`Open this URL in your browser to authenticate: ${url}`);
    const request = await waitForRequest(port);
    const code = request.query.code;
    console.log(`Oauth code`, code);
    const { tokens } = await oauth2Client.getToken(code);
    console.log(`Oauth tokens`, tokens);
    if (tokens.expiry_date) {
      console.log(`Expiration date`, new Date(tokens.expiry_date));
    }
  } else {
    throw new Error(`OAuth flow is not supported for connection kind ${connection.kind}`);
  }
}
