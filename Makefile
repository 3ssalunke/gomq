proto-compile:
	@protoc --go_out=./pkg --go-grpc_out=./pkg ./pkg/proto/*.proto

cli-proto-compile:
	@protoc --go_out=./cmd/cli --go-grpc_out=./cmd/cli ./cmd/cli/proto/*.proto

build-broker:
	@go build -o ./bin/broker.exe ./cmd/broker/main.go

build-broker-linux:
	@GOOS=linux GOARCH=amd64 go build -o ./bin/broker-linux ./cmd/broker/main.go

build-broker-macos:
	@GOOS=darwin GOARCH=amd64 go build -o ./bin/broker-macos ./cmd/broker/main.go

run-broker-build: build-broker
	@./bin/broker

build-cli:
	@go build -o ./bin/cli.exe ./cmd/cli/main.go

build-cli-linux:
	@GOOS=linux GOARCH=amd64 go build -o ./bin/cli-linux ./cmd/cli/main.go

build-cli-macos:
	@GOOS=darwin GOARCH=amd64 go build -o ./bin/cli-macos ./cmd/cli/main.go

run-cli-build: build-cli
	@./bin/cli

run-broker:
	@go run ./cmd/broker/main.go

run-cli:
	@go run ./cmd/cli/main.go