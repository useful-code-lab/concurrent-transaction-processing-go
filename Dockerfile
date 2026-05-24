# --- СТАДИЯ СБОРКИ ---
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Собираем оба бинарника
RUN go build -o /app/server ./cmd/server/main.go
RUN go build -o /app/client ./cmd/client/main.go
RUN go build -o /app/worker ./cmd/worker/main.go

# --- ОБРАЗ СЕРВЕРА ---
FROM alpine:3.20 AS server
WORKDIR /app
COPY --from=builder /app/server .
EXPOSE 8080
CMD ["./server"]

# --- ОБРАЗ КЛИЕНТА ---
FROM alpine:3.20 AS client
WORKDIR /app
COPY --from=builder /app/client .
CMD ["./client"]

# --- ОБРАЗ ВОРКЕРА (Новое) ---
FROM alpine:3.20 AS worker
WORKDIR /app
COPY --from=builder /app/worker .
CMD ["./worker"]
