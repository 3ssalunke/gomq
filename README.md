# GoMQ - A Golang Message Queue with Protobuf Support

GoMQ is a lightweight, custom message queue built in Golang, offering native Protobuf support for structured message serialization. It includes a **broker server** for managing exchanges, queues, message publishing and delivering, as well as a **CLI** for interacting with the message broker.

This repository also includes a go client API which can be used to interact with broker and perform all the broker operations.

This README provides instructions to set up the broker server and use the CLI for common tasks like creating exchanges, queues, bindings, publishing messages and setting up consumers for the queues.

## Features

- Message Broker with exchanges, queues and bindings
- Native Protobuf support for message serialization
- Consumer Groups for load balancing
- Dead Letter Queue (DLQ) support
- Message Acknowledgment and Re-delivery
- CLI to interact with the broker (create exchanges, queues, bindings, and more)

## Prerequisites

- Golang (version 1.x or higher)

## Downloading Binaries

GoMQ provides pre-built binaries for the broker server and CLI. You can download them from the [Releases](https://github.com/3ssalunke/gomq/releases) page.

### Download and Install

1. **Download the appropriate binary** for your operating system:

   - For Linux, download `broker-linux` and `cli-linux`.
   - For Windows, download `broker.exe` and `cli.exe`.
   - For Mac, download `broker-macos` and `cli-macos`.

2. **Extract the downloaded files** (if necessary) and place them in a directory included in your system's `PATH`, or run them from the downloaded location.

## Running the Broker Server

### Step 1: Run the Broker Server

Navigate to the directory where the `broker` binary is located and run:

```bash
./broker
```

By default, the server listens on localhost:50051 for incoming gRPC connections. For changing the default settings of broker, please source environment variables listed in **.env.broker.example**.

## Using the CLI

Below are some common tasks you can perform with the GoMQ CLI:

#### Create an Admin user

Navigate to the directory where the `cli` binary is located and run:

```bash
./cli create-admin -u <user-name>
```

#### Create an User

```bash
./cli create-user -u <user-name> -r <user-role>
```

#### Revoke an Apikey

```bash
./cli revoke-apikey -a <apikey>
```

#### Create an Exchange

```bash
./cli create-exchange -e <exchange-name> -t <exchange-type> -s <exchange-schema>
```

#### Remove an Exchange

```bash
./cli remove-exchange -e <exchange-name>
```

#### Create an Queue

```bash
./cli create-queue -q <queue-name> -d true -m 5
```

#### Remove an Queue

```bash
./cli remove-queue -e <exchange-name> -q <queue-name> -d true -m 5
```

#### Bind a queue

```bash
./cli bind-queue -e <exchange-name> -q <queue-name> -k <routing-key>
```

#### Publish a message

```bash
./cli publish-message -e <exchange-name> -k <routing-key> -m <json-payload>
```

#### Start a consumer

```bash
./cli start-consumer -q <queue-name>
```

#### Redrive messages to main queue

```bash
./cli redrive-messages -q <queue-name>
```

```markdown
For all commands, you can use the `--help` flag to get detailed information about the command and its usage.
```

For changing the default settings of client cli, please source environment variables listed in **.env.client.example**.

## Using GoMQ Client API

To use the GoMQ client API in your project, follow these steps:

**Install the GoMQ Client Package**:

```bash
go get github.com/3ssalunke/gomq/client
```
