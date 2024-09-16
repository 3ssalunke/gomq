proto-compile:
	@protoc --go_out=./pkg --go-grpc_out=./pkg ./pkg/proto/*.proto

build-broker:
	@go build -o ./bin/broker ./cmd/broker/main.go

run-broker-build: build-broker
	@./bin/broker

run-broker:
	@go run ./cmd/broker/main.go

run-publisher:
	@go run ./pkg/client/publisher/main.go

run-consumer:
	@go run ./pkg/client/consumer/main.go