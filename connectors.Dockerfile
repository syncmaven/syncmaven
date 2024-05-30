# docker buildx build --target twitter-ads --platform linux/arm64 -f connectors.Dockerfile --load -t syncmaven/twitter-ads-destination:latest .

FROM node:20-slim as base

RUN apt-get update -y
RUN apt-get install nano curl bash netcat-traditional procps jq unzip -y
WORKDIR /syncmaven


FROM base AS builder-base
RUN apt update && apt install -y python3 python3-pip make g++ sqlite3 libsqlite3-dev
RUN npm -g install pnpm


FROM builder-base AS builder
COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm run build
RUN rm -rf `find . -name "node_modules" -type d`
RUN pnpm install --prod


FROM builder AS resend

ENTRYPOINT [ "/syncmaven/bin/node-main", "/syncmaven/packages/connectors/resend" ]

