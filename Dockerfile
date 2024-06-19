FROM node:22-slim as base

#0.0.0 is a dev version
ARG SYNCMAVEN_VERSION=0.0.0
ENV SYNCMAVEN_VERSION=${SYNCMAVEN_VERSION}

ENV HUSKY=0

RUN apt-get update -y
RUN apt-get install nano curl bash netcat-traditional procps jq unzip -y
WORKDIR /syncmaven


FROM base AS base-builder
RUN apt update && apt install -y python3 python3-pip make g++ sqlite3 libsqlite3-dev
RUN npm -g install pnpm@8
ENV PNPM_HOME=/pnpm

FROM base-builder AS package-fetcher

COPY pnpm-lock.yaml .
RUN --mount=type=cache,target=/pnpm pnpm fetch


FROM package-fetcher AS builder

COPY . .
RUN --mount=type=cache,target=/pnpm pnpm install --offline --frozen-lockfile

RUN pnpm run build
RUN rm -rf `find . -name "node_modules" -type d`
RUN --mount=type=cache,target=/pnpm pnpm install --offline --prod  --frozen-lockfile

FROM base AS release

COPY --from=builder /syncmaven/ .
COPY --from=builder /syncmaven/packages/core/bin/action.sh .

RUN mkdir /project

ENV SYNCMAVEN_PROJECT_DIR=/project
ENV IN_DOCKER=1

ENTRYPOINT [ "/syncmaven/bin/node-main", "/syncmaven/packages/core" ]