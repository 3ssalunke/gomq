proto-compile:
	@protoc --go_out=./shared/pkg --go-grpc_out=./shared/pkg ./shared/pkg/proto/*.proto

build-broker:
	@go build -o ./bin/broker.exe ./broker/cmd/main.go

build-broker-linux:
	@GOOS=linux GOARCH=amd64 go build -o ./bin/broker-linux ./broker/cmd/main.go

build-broker-macos:
	@GOOS=darwin GOARCH=amd64 go build -o ./bin/broker-macos ./broker/cmd/main.go

run-broker-build: build-broker
	@./bin/broker

build-cli:
	@go build -o ./bin/cli.exe ./client/cmd/main.go

build-cli-linux:
	@GOOS=linux GOARCH=amd64 go build -o ./bin/cli-linux ./client/cmd/main.go

build-cli-macos:
	@GOOS=darwin GOARCH=amd64 go build -o ./bin/cli-macos ./client/cmd/main.go

run-cli-build: build-cli
	@./bin/cli

run-broker:
	@go run ./broker/cmd/main.go

run-cli:
	@go run ./client/cmd/main.go

run-tests:
	@go test -v ./...