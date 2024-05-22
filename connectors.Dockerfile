# docker buildx build --target twitter-ads --platform linux/arm64 -f connectors.Dockerfile --load -t syncmaven/twitter-ads-destination:latest .

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

FROM builder AS twitter-ads-builder

RUN pnpm install --config.dedupe-peer-dependents=false --filter ./packages/connectors/twitter-ads... --filter ./packages/node-cdk
RUN pnpm run --filter "./packages/connectors/twitter-ads" test
RUN pnpm run --filter "./packages/connectors/twitter-ads" build

FROM base AS twitter-ads
COPY --from=twitter-ads-builder /syncmaven .
WORKDIR /syncmaven/packages/connectors/twitter-ads

ENTRYPOINT [ "node", "dist/index.js" ]

FROM builder AS facebook-ads-builder

RUN pnpm install --config.dedupe-peer-dependents=false --filter ./packages/connectors/facebook-ads... --filter ./packages/node-cdk
RUN pnpm run --filter "./packages/connectors/facebook-ads" test
RUN pnpm run --filter "./packages/connectors/facebook-ads" build

FROM base AS facebook-ads
COPY --from=facebook-ads-builder /syncmaven/packages/connectors/facebook-ads/dist .
ENTRYPOINT [ "node", "dist/main.js" ]

FROM builder AS resend-builder

RUN pnpm install --config.dedupe-peer-dependents=false --filter ./packages/connectors/resend... --filter ./packages/node-cdk
RUN pnpm run --filter "./packages/connectors/resend" test
RUN pnpm run --filter "./packages/connectors/resend" build

FROM base AS resend
COPY --from=resend-builder /syncmaven/packages/connectors/resend/dist .
ENTRYPOINT [ "node", "dist/main.js" ]
