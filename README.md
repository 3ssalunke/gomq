# GoMQ - A Golang Message Queue with Protobuf Support

GoMQ is a lightweight, custom message queue playground built in Golang, offering native Protobuf support for structured message serialization. It includes a **broker server** for managing exchanges, queues, and message publishing, as well as a **CLI** for interacting with the message broker.

This README provides instructions to set up the broker server and use the CLI for common tasks like creating exchanges, queues, binding queues, publishing messages and setting up consumers to the queues.

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
git clone https://github.com/yourusername/gomq.git
cd gomq
```

### Install Dependencies

```bash
go mod tidy
```

### Running the Broker Server

````markdown
## Running the Broker Server

### Step 1: Compile the Broker Server

```bash
go build -o ./bin/broker.exe ./cmd/broker/main.go
```
````

### Step 2: Compile the Broker Server

```
./bin/broker
```

### Using the CLI

````markdown
## Using the GoMQ CLI

The GoMQ CLI provides an interface for interacting with the broker, allowing you to create exchanges, queues, bindings, publish messages and setup consumers.

### Step 1: Compile the CLI

```bash
go build -o gomq-cli ./cmd/cli.go
```
````
