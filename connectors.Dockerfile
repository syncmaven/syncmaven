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
RUN pnpm install
RUN pnpm build
RUN rm -rf `find . -name "node_modules" -type d`
RUN pnpm install --prod


FROM builder AS resend

COPY --from=builder /syncmaven/ .
#RUN rm -rf packages/core

ENTRYPOINT [ "node", "dist/index.js" ]

FROM builder AS resend-builder

RUN pnpm install --config.dedupe-peer-dependents=false --filter ./packages/connectors/resend... --filter ./packages/node-cdk
RUN pnpm run --filter "./packages/connectors/resend" test
RUN pnpm run --filter "./packages/connectors/resend" build

FROM base AS resend
COPY --from=resend-builder /syncmaven/packages/connectors/resend/dist .
ENTRYPOINT [ "node", "dist/index.js" ]
