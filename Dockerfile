FROM golang:1.21-alpine AS build

WORKDIR /app

COPY . .

RUN rm -rf client

RUN go mod tidy

RUN GOOS=linux GOARCH=amd64 go build -o ./bin/broker-linux ./broker/cmd/main.go


FROM alpine:latest

WORKDIR /root/

# temp
# RUN apk update && apk add --no-cache curl
# RUN curl -LO https://github.com/fullstorydev/grpcurl/releases/download/v1.8.7/grpcurl_1.8.7_linux_x86_64.tar.gz
# RUN tar -xvzf grpcurl_1.8.7_linux_x86_64.tar.gz
# RUN mv grpcurl /usr/local/bin/

COPY --from=build /app/bin/broker-linux .
COPY --from=build /app/.env.broker .env.broker

EXPOSE 50051

CMD ["./broker-linux"]
