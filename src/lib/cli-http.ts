import express from "express";
import http from "http";

export function waitForRequest(port: number): Promise<express.Request> {
  return new Promise((resolve, reject) => {
    const app = express();
    const server = http.createServer(app);

    app.use((req, res, next) => {
      resolve(req); // Resolve the promise with the request object
      res.status(200).send("Please, see your token in the console. You can close this window");
      server.close(err => {
        // Close the server
        if (err) {
          reject(err);
        }
      });
    });

    server.listen(port, () => {
      //console.log(`Server is running on http://localhost:${port}`);
    });

    server.on("error", error => {
      reject(error);
    });
  });
}
