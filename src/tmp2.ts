/*
 * To keep this code short I am not dealing with error handling, security details, etc.
 */

const Dockerode = require('dockerode');

const run = async (stdin) => {
  const docker = new Dockerode({ socketPath: '/var/run/docker.sock' });

  // Create the container.
  const container = await docker.createContainer({
    AttachStderr: true,
    AttachStdin: true,
    AttachStdout: true,
    Cmd: ['tr', '[a-z]', '[A-z]'],
    Image: 'alpine:latest',
    OpenStdin: true,
    StdinOnce: true,
    Tty: false,
  });

  // Attach the container.
  const stream = await container.attach({
    hijack: true,
    stderr: true,
    stdin: true,
    stdout: true,
    stream: true,
  });

  // Promisify stream callback response to be able to return its value.
  const stdout = new Promise((resolve) => {
    stream.on('data', (data) => {
      // The first 8 bytes are used to define the response header.
      // Please refer to https://docs.docker.com/engine/api/v1.37/#operation/ContainerAttach
      const response = data && data.slice(8).toString();

      resolve(response);
    });
  });

  stream.write(stdin);

  // Start the container.
  await container.start();

  // We need this and the hijack flag so we can signal the end of the stdin input for the container
  // while still being able to receive the stdout response.
  stream.end();

  // We wait for container response.
  await container.wait();

  // We remove the container after its execution. This is the same as `--rm`.
  container.remove();

  return stdout;
};

(async () => {
  const stdin = 'hello world';
  const stdout = await run(stdin);

  console.log(stdout);
})();

