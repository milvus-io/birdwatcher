FROM golang:1.24-bookworm AS build

WORKDIR /birdwatcher

# Install build prerequisites (CGO + Kafka)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        pkg-config \
        librdkafka-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Enable CGO and set module proxy
ENV CGO_ENABLED=1
ENV GOPROXY="https://goproxy.cn,direct"

# Cache modules first
COPY go.mod go.sum ./
RUN go mod download

# Then copy the rest of the source
COPY . .


RUN go build -o /birdwatcher/bin/birdwatcher cmd/birdwatcher/main.go

FROM debian:bookworm-slim

# Install runtime dependencies for Kafka client
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /birdwatcher/bin/birdwatcher /birdwatcher/birdwatcher

CMD ["sleep", "infinity"]
