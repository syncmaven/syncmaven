# docker buildx build --target twitter-ads --platform linux/arm64 -f connectors.Dockerfile --load -t syncmaven/twitter-ads-destination:latest .

FROM node:20-slim as base
ENV HUSKY=0

RUN apt-get update -y
RUN apt-get install nano curl bash netcat-traditional procps jq unzip -y
WORKDIR /syncmaven


FROM base AS builder-base
RUN apt update && apt install -y python3 python3-pip make g++ sqlite3 libsqlite3-dev
RUN npm -g install pnpm
ENV PNPM_HOME=/pnpm

FROM builder-base AS package-fetcher

COPY pnpm-lock.yaml .
RUN --mount=type=cache,target=/pnpm pnpm fetch


FROM package-fetcher AS builder
COPY . .
RUN --mount=type=cache,target=/pnpm pnpm install --frozen-lockfile --offline
RUN pnpm run build
RUN rm -rf `find . -name "node_modules" -type d`
RUN --mount=type=cache,id=pnpm,target=/pnpm pnpm install --offline --prod


FROM base AS release

COPY --from=builder /syncmaven/ .

