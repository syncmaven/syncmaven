# See ./docker-build-core.sh for instructions on how to build this image


FROM node:20-slim as base

RUN apt-get update -y
RUN apt-get install nano curl bash netcat-traditional procps jq unzip -y
WORKDIR /syncmaven


FROM base AS base-builder
RUN apt update && apt install -y python3 python3-pip make g++ sqlite3 libsqlite3-dev
RUN npm -g install pnpm@8

FROM base-builder AS builder

COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm run build
RUN rm -rf `find . -name "node_modules" -type d`
RUN pnpm install --prod

FROM base AS release

COPY --from=builder /syncmaven/ .
COPY --from=builder /syncmaven/packages/core/bin/action.sh .

RUN mkdir /project
ENV SYNCMAVEN_PROJECT_DIR=/project

ENTRYPOINT [ "node", "/syncmaven/packages/core/dist/src/index.js" ]