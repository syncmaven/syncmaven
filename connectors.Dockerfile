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
RUN pnpm install
RUN pnpm run --filter "./packages/connectors/**" test # verify once again so we are sure not to ship a broken image
RUN pnpm run --filter "./packages/connectors/**" build


FROM base AS twitter-ads
COPY --from=builder /syncmaven/packages/connectors/twitter-ads/dist .
ENTRYPOINT [ "node", "main.js" ]

FROM base AS facebook-ads
COPY --from=builder /syncmaven/packages/connectors/facebook-ads/dist .
ENTRYPOINT [ "node", "main.js" ]

FROM base AS resend
COPY --from=builder /syncmaven/packages/connectors/resend/dist .
ENTRYPOINT [ "node", "main.js" ]
