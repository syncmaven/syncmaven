# First stage: build Go dependencies
FROM golang:1.22.3-alpine as deps

RUN mkdir /app
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

# Second stage: build the application
FROM golang:1.22.3-alpine as build

RUN mkdir /app
WORKDIR /app

COPY . .
COPY --from=deps /go/pkg /go/pkg

# Build the application
RUN go build -o mixpanel

# Final stage: create the runtime image
FROM alpine as final

ENV TZ=UTC

RUN mkdir /app
WORKDIR /app

# Copy the built application from the build stage
COPY --from=build /app/mixpanel ./

ENTRYPOINT ["/app/mixpanel"]