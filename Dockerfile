FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o hive-serving .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /build/hive-serving /app/hive-serving
COPY web/ /app/web/

EXPOSE 8080

ENV LISTEN_ADDR=:8080
ENV OLLAMA_ADDR=http://host.docker.internal:11434
ENV LONEWOLF_URL=http://host.docker.internal:8088

WORKDIR /app

CMD ["/app/hive-serving"]
