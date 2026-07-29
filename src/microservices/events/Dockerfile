FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o events-service .

FROM alpine:3.21

RUN apk --no-cache add ca-certificates \
    && addgroup -S events \
    && adduser -S -G events events

WORKDIR /app

COPY --from=builder /app/events-service .

USER events

EXPOSE 8082

CMD ["./events-service"]
