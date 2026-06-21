FROM golang:1.26.3-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o taskforge ./cmd/server

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/taskforge ./taskforge

EXPOSE 8080

CMD ["./taskforge"]