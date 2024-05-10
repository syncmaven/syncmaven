FROM node:20-slim as base

RUN apt-get update -y
RUN apt-get install nano curl bash netcat-traditional procps jq unzip -y
#RUN curl -fsSL https://bun.sh/install | bash -s "bun-v1.0.16"
#RUN ln -s `which bun` /bin/bun
WORKDIR /syncmaven


FROM base AS builder
RUN apt update && apt install -y python3 python3-pip make g++ sqlite3 libsqlite3-dev
RUN npm -g install pnpm
RUN curl -fsSL https://bun.sh/install | bash -s "bun-v1.0.16"
RUN ln -s `which bun` /bin/bun
COPY . .
RUN pnpm install
#RUN pnpm run test - this is not working by some reason
RUN pnpm run build

FROM base AS release
COPY --from=builder /syncmaven/node_modules node_modules
COPY --from=builder /syncmaven/dist/src .
RUN mkdir /project

ENV SYNCMAVEN_PROJECT_DIR=/project
ENTRYPOINT [ "node", "index.js" ]