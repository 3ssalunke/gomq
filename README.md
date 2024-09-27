# GoMQ - A Golang Message Queue with Protobuf Support

GoMQ is a lightweight, custom message queue playground built in Golang, offering native Protobuf support for structured message serialization. It includes a **broker server** for managing exchanges, queues, message publishing and delivering, as well as a **CLI** for interacting with the message broker.

This README provides instructions to set up the broker server and use the CLI for common tasks like creating exchanges, queues, bindings, publishing messages and setting up consumers for the queues.

## Features

- Message Broker with exchanges, queues and bindings
- Native Protobuf support for message serialization (TODO)
- Consumer Groups for load balancing
- Dead Letter Queue (DLQ) support
- Message Acknowledgment and Re-delivery
- CLI to interact with the broker (create exchanges, queues, bindings, and more)

## Prerequisites

- Golang (version 1.x or higher)

## Installation

### Clone the Repository

```bash
git clone https://github.com/3ssalunke/gomq.git
cd gomq
```

### Install Dependencies

```bash
go mod tidy
```

### Running the Broker Server

#### Step 1: Compile the Broker Server

```bash
go build -o ./bin/broker.exe ./cmd/broker/main.go
```

#### Step 2: Run the Broker Server

```bash
./bin/broker
```

By default, the server listens on localhost:50051 for incoming gRPC connections.

## Using the CLI

The GoMQ CLI provides an interface for interacting with the broker, allowing you to create exchanges, queues, bindings, publish messages and setup consumers.

### Step 1: Compile the CLI

```bash
go build -o ./bin/cli.exe ./cmd/cli/main.go
```

### Step 2: CLI Usage

Below are some common tasks you can perform with the GoMQ CLI:

#### Create an Exchange

```bash
./bin/cli create-exchange -e ExchangeName -t ExchangeType
```

#### Create an Queue

```bash
./bin/cli create-queue -q QueueName -d true -m 5
```

#### Bind a queue

```bash
./bin/cli bind-queue -e ExchangeName -q QueueName -k RoutingKey
```

#### Publish a message

```bash
./bin/cli publish-message -e ExchangeName -k RoutingKey -m "Message Payload (JSON)"
```

#### Start a consumer

```bash
./bin/cli start-consumer -q QueueName
```

```markdown
For all commands, you can use the `--help` flag to get detailed information about the command and its usage.
```
