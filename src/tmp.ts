import Docker from "dockerode";

// Initialize dockerode
const docker = new Docker({ socketPath: "/var/run/docker.sock" });

/**
 * Function to run a Docker container and interact with it via stdin and stdout.
 * @param imageName The name of the Docker image to run.
 * @param command The command to run in the Docker container.
 */
async function runDockerContainer(imageName: string, command: string[]): Promise<void> {
  try {
    // Pull the image if it's not already present
    console.log(`Pulling image ${imageName}...`);
    await docker.pull(imageName);
    console.log(`Image ${imageName} pulled.`);

    // Create and start the container
    const container = await docker.createContainer({
      Image: imageName,
      Entrypoint: command,
      AttachStdin: true,
      AttachStdout: true,
      AttachStderr: true,
      OpenStdin: true,
      Tty: false,
    });
    console.log("Container created.");
    const clear = async () => {
      if (container) {
        try {
          console.log(`Stopping container ${container.id}...`);
          await container.stop();
        } catch (error) {
          //
        }
        try {
          console.log(`Removing container ${container.id}...`);
          await container.remove();
        } catch (error) {
          //
        }
        console.log("All done");
      }
    };

    // process.on("SIGINT", async () => {
    //   console.log("Received SIGINT. Removing container...");
    //   await clear();
    // });
    process.on("SIGTERM", async () => {
      console.log("Received SIGTERM. Removing container...");
      await clear();
      process.exit(0);
    });

    const containerStream = await container.attach({
      hijack: true,
      stderr: true,
      stdin: true,
      stdout: true,
      stream: true,
    });
    console.log("Stream attached.");

    // Start the container
    await container.start();
    console.log("Container started.");

    // Write to container's stdin
    containerStream.write("echo Hello, Docker!\n");
    console.log("Message sent to container.");

    containerStream.on('data', (chunk) => {
      console.log(`REPL:` + chunk.toString());
    });
    //containerStream.pipe(process.stdout);
    await new Promise(resolve => setTimeout(resolve, 1000)); // Wait for the output to be printed
    // console.log("Output ended");
    // containerStream.end(); // This ensures that the stdin stream is closed after sending the input
    //
    // await clear();

  } catch (error) {
    console.error("Error:", error);
  }
}

// Example usage
runDockerContainer("ubuntu", ["/bin/bash"]);