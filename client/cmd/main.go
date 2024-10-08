package main

import (
	"log"

	"github.com/3ssalunke/gomq/client/internal/config"
	client "github.com/3ssalunke/gomq/client/pkg"
	"github.com/3ssalunke/gomq/shared/util"
	"github.com/spf13/cobra"
)

type GoMQCLI struct {
	client  *client.MQClient
	rootCmd *cobra.Command
}

func NewGoMQCLI(gomqClient *client.MQClient) *GoMQCLI {
	cli := &GoMQCLI{
		client: gomqClient,
		rootCmd: &cobra.Command{
			Use:   "gomq-cli",
			Short: "CLI tool for GoMQ",
		},
	}

	cli.rootCmd.AddCommand(cli.createAdmin())
	cli.rootCmd.AddCommand(cli.createUser())
	cli.rootCmd.AddCommand(cli.createExchangeCmd())
	cli.rootCmd.AddCommand(cli.removeExchangeCmd())
	cli.rootCmd.AddCommand(cli.createQueueCmd())
	cli.rootCmd.AddCommand(cli.removeQueueCmd())
	cli.rootCmd.AddCommand(cli.bindQueueCmd())
	cli.rootCmd.AddCommand(cli.cliPublishMessageCmd())
	cli.rootCmd.AddCommand(cli.publishMessageCmd())
	cli.rootCmd.AddCommand(cli.retrieveMessagesCmd())
	cli.rootCmd.AddCommand(cli.startCliConsumerCmd())
	cli.rootCmd.AddCommand(cli.startConsumerCmd())
	cli.rootCmd.AddCommand(cli.redriveDlqMessagesCmd())

	return cli
}

func main() {
	config := config.LoadConfig()

	gomqClient := client.NewMQClient(config)

	cli := NewGoMQCLI(gomqClient)

	if err := cli.rootCmd.Execute(); err != nil {
		log.Fatal(err.Error())
	}
}

func (cli *GoMQCLI) createAdmin() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create-admin",
		Short: "Create a new admin",
		Run: func(cmd *cobra.Command, args []string) {
			username, _ := cmd.Flags().GetString("user-name")

			if username == "" {
				log.Fatal("please provide user name")
			}

			msg, err := cli.client.CreateAdmin(username)
			if err != nil {
				log.Fatal("error while creating admin", err.Error())
			}
			log.Println("create-admin message", msg)
		},
	}

	cmd.Flags().StringP("user-name", "u", "", "Name of the admin")

	return cmd
}

func (cli *GoMQCLI) createUser() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create-user",
		Short: "Create a new user",
		Run: func(cmd *cobra.Command, args []string) {
			username, _ := cmd.Flags().GetString("user-name")
			role, _ := cmd.Flags().GetString("user-role")

			if username == "" {
				log.Fatal("please provide user name")
			}
			if role == "" {
				log.Fatal("please provide user role")
			}

			msg, err := cli.client.CreateUser(username, role)
			if err != nil {
				log.Fatal("error while creating user", err.Error())
			}
			log.Println("create-user message", msg)
		},
	}

	cmd.Flags().StringP("user-name", "u", "", "Name of the user")
	cmd.Flags().StringP("user-role", "r", "", "Role of the user")

	return cmd
}

func (cli *GoMQCLI) createExchangeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create-exchange",
		Short: "Create a new exchange",
		Run: func(cmd *cobra.Command, args []string) {
			name, _ := cmd.Flags().GetString("exchange-name")
			extype, _ := cmd.Flags().GetString("exchange-type")
			schema, _ := cmd.Flags().GetString("exchange-schema")

			if name == "" {
				log.Fatal("please provide exchange name")
			}
			if extype == "" {
				log.Fatal("please provide exchange type")
			}
			if schema == "" {
				log.Fatal("please provide exchange schema")
			}

			msg, err := cli.client.CreateExchange(name, extype, schema)
			if err != nil {
				log.Fatal("error while creating exchange", err.Error())
			}
			log.Println("create-exchange message", msg)
		},
	}

	cmd.Flags().StringP("exchange-name", "e", "", "Name of the exchange")
	cmd.Flags().StringP("exchange-type", "t", "", "Type of the exchange")
	cmd.Flags().StringP("exchange-schema", "s", "", "Schema string for exchange")

	return cmd
}

func (cli *GoMQCLI) removeExchangeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-exchange",
		Short: "Remove a new exchange from broker",
		Run: func(cmd *cobra.Command, args []string) {
			name, _ := cmd.Flags().GetString("exchange-name")
			if name == "" {
				log.Fatal("please provide exchange name")
			}

			msg, err := cli.client.RemoveExchange(name)
			if err != nil {
				log.Fatal("error while removing exchange", err.Error())
			}
			log.Println("remove-exchange message", msg)
		},
	}

	cmd.Flags().StringP("exchange-name", "e", "", "Name of the exchange")

	return cmd
}

func (cli *GoMQCLI) createQueueCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create-queue",
		Short: "Create a new queue",
		Run: func(cmd *cobra.Command, args []string) {
			name, _ := cmd.Flags().GetString("queue-name")
			dlq, _ := cmd.Flags().GetBool("queue-dlq")
			maxRetries, _ := cmd.Flags().GetInt8("max-retries")

			if name == "" {
				log.Fatal("please provide queue name")
			}
			if dlq && maxRetries == 0 {
				log.Fatal("please enter max retries count for message")
			}

			msg, err := cli.client.CreateQueue(name, dlq, maxRetries)
			if err != nil {
				log.Fatal("error while creating queue", err.Error())
			}
			log.Println("create-queue message", msg)
		},
	}

	cmd.Flags().StringP("queue-name", "q", "", "Name of the queue")
	cmd.Flags().BoolP("queue-dlq", "d", false, "Dead letter queue")
	cmd.Flags().Int8P("max-retries", "r", 0, "Max retries for Dead letter")

	return cmd
}

func (cli *GoMQCLI) removeQueueCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-queue",
		Short: "Remove an queue from exchange",
		Run: func(cmd *cobra.Command, args []string) {
			queueName, _ := cmd.Flags().GetString("queue-name")
			exchangeName, _ := cmd.Flags().GetString("exchange-name")
			if queueName == "" {
				log.Fatal("please provide queue name")
			}
			if exchangeName == "" {
				log.Fatal("please provide exchange name")
			}

			msg, err := cli.client.RemoveQueue(exchangeName, queueName)
			if err != nil {
				log.Fatal("error while removing queue", err.Error())
			}
			log.Println("remove-queue message", msg)
		},
	}

	cmd.Flags().StringP("queue-name", "q", "", "Name of the queue")
	cmd.Flags().StringP("exchange-name", "e", "", "Name of the exchange")

	return cmd
}

func (cli *GoMQCLI) bindQueueCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bind-queue",
		Short: "Bind an queue to exchange",
		Run: func(cmd *cobra.Command, args []string) {
			exchangeName, _ := cmd.Flags().GetString("exchange-name")
			queueName, _ := cmd.Flags().GetString("queue-name")
			routingKey, _ := cmd.Flags().GetString("routing-key")
			if exchangeName == "" {
				log.Fatal("please provide exchange name")
			}
			if queueName == "" {
				log.Fatal("please provide queue name")
			}
			if routingKey == "" {
				log.Fatal("please provide routing key")
			}

			msg, err := cli.client.BindQueue(exchangeName, queueName, routingKey)
			if err != nil {
				log.Fatal("error while queue binding", err.Error())
			}
			log.Println("bind-queue message", msg)
		},
	}

	cmd.Flags().StringP("exchange-name", "e", "", "Name of the exchange")
	cmd.Flags().StringP("queue-name", "q", "", "Name of the queue")
	cmd.Flags().StringP("routing-key", "k", "", "Routing key for binding")

	return cmd
}

func (cli *GoMQCLI) cliPublishMessageCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "publish-message",
		Short: "Publish message to exchange",
		Run: func(cmd *cobra.Command, args []string) {
			exchangeName, _ := cmd.Flags().GetString("exchange-name")
			routingKey, _ := cmd.Flags().GetString("routing-key")
			message, _ := cmd.Flags().GetBytesBase64("message")

			if exchangeName == "" {
				log.Fatal("please provide exchange name")
			}
			if routingKey == "" {
				log.Fatal("please provide routing key")
			}
			if message == nil {
				log.Fatal("please provide protobuf message bytes")
			}

			msg, err := cli.client.CliPublishMessage(exchangeName, routingKey, message)
			if err != nil {
				log.Fatal("error while publishing message", err.Error())
			}
			log.Println("publish-message message", msg)
		},
	}

	cmd.Flags().StringP("exchange-name", "e", "", "Name of the exchange")
	cmd.Flags().StringP("routing-key", "k", "", "Routing key for binding")
	cmd.Flags().BytesBase64P("message", "m", nil, "Protobuf message bytes")

	return cmd
}

func (cli *GoMQCLI) publishMessageCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test-publish-message",
		Short: "Publish message to exchange (test)",
		Run: func(cmd *cobra.Command, args []string) {
			exchangeName, _ := cmd.Flags().GetString("exchange-name")
			routingKey, _ := cmd.Flags().GetString("routing-key")

			if exchangeName == "" {
				log.Fatal("please provide exchange name")
			}
			if routingKey == "" {
				log.Fatal("please provide routing key")
			}

			message := struct {
				Name  string
				Id    int32
				Email string
			}{
				Name:  "test",
				Id:    int32(util.GenerateRandomInt()),
				Email: "test@test.com",
			}

			msg, err := cli.client.PublishMessage(exchangeName, routingKey, message)
			if err != nil {
				log.Fatal("error while publishing message ", err.Error())
			}
			log.Println("publish-message message", msg)
		},
	}

	cmd.Flags().StringP("exchange-name", "e", "", "Name of the exchange")
	cmd.Flags().StringP("routing-key", "k", "", "Routing key for binding")

	return cmd
}

func (cli *GoMQCLI) retrieveMessagesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retrieve-messages",
		Short: "Retrieve messages from queue",
		Run: func(cmd *cobra.Command, args []string) {
			queueName, _ := cmd.Flags().GetString("queue-name")
			count, _ := cmd.Flags().GetInt32("message-count")
			if queueName == "" {
				log.Fatal("please provide queue name")
			}
			if count == 0 {
				log.Fatal("please provide valid message count(>0)")
			}

			msg, err := cli.client.RetrieveMessages(queueName, count)
			if err != nil {
				log.Fatal("error while retrieving queue messages", err.Error())
			}
			log.Println("retrieve-messages message", msg)
		},
	}

	cmd.Flags().StringP("queue-name", "q", "", "Name of the queue")
	cmd.Flags().Int32P("message-count", "c", 0, "Message count to be retrieved")

	return cmd
}

func (cli *GoMQCLI) startCliConsumerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start-consumer",
		Short: "Start queue consumer",
		Run: func(cmd *cobra.Command, args []string) {
			queueName, _ := cmd.Flags().GetString("queue-name")
			if queueName == "" {
				log.Fatal("please provide queue name")
			}

			if err := cli.client.CliStartConsumer(queueName); err != nil {
				log.Fatal("error while consuming queue messages: ", err.Error())
			}
		},
	}

	cmd.Flags().StringP("queue-name", "q", "", "Name of the queue")

	return cmd
}

func (cli *GoMQCLI) startConsumerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test-start-consumer",
		Short: "Start queue consumer (test)",
		Run: func(cmd *cobra.Command, args []string) {
			queueName, _ := cmd.Flags().GetString("queue-name")
			exchangeName, _ := cmd.Flags().GetString("exchange-name")
			if queueName == "" {
				log.Fatal("please provide queue name")
			}
			if exchangeName == "" {
				log.Fatal("please provide exchange name")
			}

			var message struct {
				Name  string
				Id    int32
				Email string
			}

			if err := cli.client.StartConsumer(exchangeName, queueName, &message); err != nil {
				log.Fatal("error while consuming queue messages: ", err.Error())
			}
		},
	}

	cmd.Flags().StringP("exchange-name", "e", "", "Name of the exchange")
	cmd.Flags().StringP("queue-name", "q", "", "Name of the queue")

	return cmd
}

func (cli *GoMQCLI) redriveDlqMessagesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "redrive-messages",
		Short: "Redrive DLQ messages to parent queue",
		Run: func(cmd *cobra.Command, args []string) {
			queueName, _ := cmd.Flags().GetString("queue-name")
			if queueName == "" {
				log.Fatal("plase provide queue name")
			}

			_, err := cli.client.RedriveDlqMessages(queueName)
			if err != nil {
				log.Fatal("error while redriving dlq messages: ", err.Error())
			}
		},
	}

	cmd.Flags().StringP("queue-name", "q", "", "Name of the queue")

	return cmd
}
