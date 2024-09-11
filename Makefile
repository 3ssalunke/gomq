proto-compile:
	@protoc --go_out=./pkg --go-grpc_out=./pkg ./pkg/proto/message.proto

build-broker:
	@go build -o ./bin/broker ./cmd/main.go

run-broker-build: build-broker
	@./bin/broker

run-broker:
	@go run ./cmd/main.go

build-client:
	@go build -o ./bin/client ./client/main.go

run-client-build: build-client
	@./bin/client

run-client:
	@go run ./client/main.go