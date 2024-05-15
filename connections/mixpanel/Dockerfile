# docker build -t syncmaven/connection-mixpanel:dev --load .

FROM golang:1.22.3-alpine as build

RUN mkdir /app
WORKDIR /app

COPY . .
# Build bulker
RUN go build -o mixpanel

#######################################
# FINAL STAGE
FROM alpine as final

ENV TZ=UTC

RUN mkdir /app
WORKDIR /app

# Copy bulkerapp
COPY --from=build /app/mixpanel ./

ENTRYPOINT ["/app/mixpanel"]