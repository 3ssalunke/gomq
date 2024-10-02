proto-compile:
	@protoc --go_out=./pkg --go-grpc_out=./pkg ./pkg/proto/*.proto

cli-proto-compile:
	@protoc --go_out=./cmd/cli --go-grpc_out=./cmd/cli ./cmd/cli/proto/*.proto

build-broker:
	@go build -o ./bin/broker.exe ./cmd/broker/main.go

run-broker-build: build-broker
	@./bin/broker

build-cli:
	@go build -o ./bin/cli.exe ./cmd/cli/main.go

run-cli-build: build-cli
	@./bin/cli

run-broker:
	@go run ./cmd/broker/main.go

run-cli:
	@go run ./cmd/cli/main.go