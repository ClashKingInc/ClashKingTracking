# syntax=docker/dockerfile:1.7

FROM golang:1.26.4-bookworm AS build

WORKDIR /src/PycharmProjects/clashking_tracking

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -o /out/tracking .

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /out/tracking /usr/local/bin/tracking
COPY config.json /app/config.json

ENTRYPOINT ["tracking"]
