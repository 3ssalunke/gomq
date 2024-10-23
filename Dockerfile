FROM golang:1.21-alpine AS build

WORKDIR /app

COPY . .

RUN rm -rf client

RUN go mod tidy

RUN GOOS=linux GOARCH=amd64 go build -o ./bin/broker-linux ./broker/cmd/main.go


FROM alpine:latest

WORKDIR /root/

ENV PROTOC_VERSION=28.3

RUN apk add --no-cache \
    bash \
    git \
    curl \
    unzip \
    build-base \
    protobuf-dev

RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip && \
    unzip protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /usr/local && \
    rm -f protoc-${PROTOC_VERSION}-linux-x86_64.zip

COPY --from=build /app/bin/broker-linux .
COPY --from=build /app/.env.broker .env.broker

EXPOSE 50051

CMD ["./broker-linux"]
