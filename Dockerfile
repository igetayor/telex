# =========================================================================
# Multi-Stage Production Dockerfile for Telex Event Streaming Worker
# =========================================================================

# Stage 1: Build & Static Compilation
FROM golang:1.24-alpine AS builder

WORKDIR /build

RUN apk update && apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w -extldflags '-static'" \
    -o /build/bin/telex \
    ./cmd/worker

# Stage 2: Minimal Distroless/Alpine Production Runtime
FROM alpine:3.20

RUN addgroup -S telex && adduser -S telexuser -G telex

RUN apk --no-cache add ca-certificates tzdata wget

WORKDIR /app

COPY --from=builder /build/bin/telex /app/telex

RUN chown -R telexuser:telex /app

USER telexuser

EXPOSE 8082

ENV PORT=8082 \
    LOG_LEVEL=INFO \
    NUM_WORKERS=8 \
    QUEUE_CAPACITY=1000 \
    MAX_RETRIES=3 \
    AMQP_URL=amqp://guest:guest@rabbitmq:5672/

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8082/healthz || exit 1

ENTRYPOINT ["/app/telex"]
