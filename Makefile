proto-compile:
	@protoc --go_out=./pkg --go-grpc_out=./pkg ./pkg/proto/*.proto

build-broker:
	@go build -o ./bin/broker ./cmd/main.go

run-broker-build: build-broker
	@./bin/broker

run-broker:
	@go run ./cmd/main.go

run-publisher:
	@go run ./test/publisher/main.go

run-consumer:
	@go run ./test/consumer/main.go