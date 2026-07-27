FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o proxy-service .

FROM alpine:3.21

RUN apk --no-cache add ca-certificates \
    && addgroup -S proxy \
    && adduser -S -G proxy proxy

WORKDIR /app

COPY --from=builder /app/proxy-service .

USER proxy

EXPOSE 8000

CMD ["./proxy-service"]
