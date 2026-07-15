# Multi-stage build for hive-server-go
# Supports Linux (CPU + Nvidia GPU), macOS, and Windows via Docker
#
# Build context must be the repo root, not the hive-server-go directory.
# Correct command from repo root:
#   docker build -t hive-server-go -f hive-server-go/Dockerfile .
# Wrong command that causes missing file errors:
#   docker build -t hive-server-go -f hive-server-go/Dockerfile hive-server-go
#
# Nvidia GPU support (Linux only):
#   docker run --gpus all -p 8081:8081 ... hive-server-go:latest
# The --gpus all flag makes nvidia-smi available inside the container
# via the nvidia-container-toolkit runtime.

FROM golang:1.26-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
COPY hive-server-go/ ./hive-server-go/
RUN cd hive-server-go && go build -o hive-server-go .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /build/hive-server-go/hive-server-go /app/hive-server-go
COPY hive-server-go/static/index.html /app/static/index.html

EXPOSE 8081
EXPOSE 8082/udp

ENV OLLAMA_BASE_URL=http://host.docker.internal:11434
ENV OLLAMA_MODEL=llama3.1:8b
ENV SERVER_PORT=8081
ENV MAX_CONCURRENT=2
ENV MAX_CLIENTS=5
ENV MESH_ENABLED=true
ENV MESH_DISCOVERY_PORT=8082
ENV MESH_SEED_PEERS=

WORKDIR /app

CMD ["/app/hive-server-go"]
