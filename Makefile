protocompile:
	@protoc --go_out=./pkg --go-grpc_out=./pkg ./proto/message.proto

build:
	@go build ./cmd/main.go -o ./bin

runb: build
	@./bin

run:
	@go run ./cmd/main.go