#docker buildx build --platform linux/amd64,linux/arm64 -t syncmaven/syncmaven:latest --push .


FROM node:20-slim as base

RUN apt-get update -y
RUN apt-get install nano curl bash netcat-traditional procps jq unzip -y
#RUN curl -fsSL https://bun.sh/install | bash -s "bun-v1.0.16"
#RUN ln -s `which bun` /bin/bun
WORKDIR /syncmaven


FROM base AS builder
RUN apt update && apt install -y python3 python3-pip make g++ sqlite3 libsqlite3-dev
RUN npm -g install pnpm

COPY . .
RUN pnpm install
RUN pnpm run build
# not running test here to save time. This should be run in CI mainly, which does the test separately
#RUN pnpm run test

# install deps that cannot be handled by webpack
COPY /packages/core/package.webpack.json /syncmaven/packages/core/dist/package.json
WORKDIR /syncmaven/packages/core/dist
RUN npm install


FROM base AS release

COPY --from=builder /syncmaven/packages/core/dist/ .
COPY --from=builder /syncmaven/packages/core/bin/entrypoint.sh .
COPY --from=builder /syncmaven/packages/core/bin/action.sh .


RUN mkdir /project
ENV SYNCMAVEN_PROJECT_DIR=/project

ENTRYPOINT [ "./entrypoint.sh" ]