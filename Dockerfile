FROM golang:1.21-alpine AS build

WORKDIR /app

COPY . .

RUN rm -rf client

RUN go mod tidy

RUN GOOS=linux GOARCH=amd64 go build -o ./bin/broker-linux ./broker/cmd/main.go


FROM alpine:latest

WORKDIR /root/

COPY --from=build /app/bin/broker-linux .
COPY --from=build /app/.env.broker .env.broker

EXPOSE 50051

CMD ["./broker-linux"]
