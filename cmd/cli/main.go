package main

import (
	"log"

	"github.com/3ssalunke/gomq/pkg/client"
	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{Use: "gomq-cli"}

	rootCmd.AddCommand(createExchangeCmd)
	rootCmd.AddCommand(createQueueCmd)
	rootCmd.AddCommand(bindQueueCmd)
	rootCmd.AddCommand(publishMessageCmd)
	rootCmd.AddCommand(retrieveMessagesCmd)
	rootCmd.AddCommand(startConsumerCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err.Error())
	}
}

var createExchangeCmd = &cobra.Command{
	Use:   "create-exchange",
	Short: "Create a new exchange",
	Run: func(cmd *cobra.Command, args []string) {
		name, _ := cmd.Flags().GetString("exchange-name")
		if name == "" {
			log.Fatal("please provide exchange name")
		}
		msg, err := client.CreateExchange(name)
		if err != nil {
			log.Fatal("error while creating exchange", err.Error())
		}
		log.Println("create-exchange message", msg)
	},
}

var createQueueCmd = &cobra.Command{
	Use:   "create-queue",
	Short: "Create a new queue",
	Run: func(cmd *cobra.Command, args []string) {
		name, _ := cmd.Flags().GetString("queue-name")
		if name == "" {
			log.Fatal("please provide queue name")
		}
		msg, err := client.CreateQueue(name)
		if err != nil {
			log.Fatal("error while creating exchange", err.Error())
		}
		log.Println("create-queue message", msg)
	},
}

var bindQueueCmd = &cobra.Command{
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
		msg, err := client.BindQueue(exchangeName, queueName, routingKey)
		if err != nil {
			log.Fatal("error while queue binding", err.Error())
		}
		log.Println("bind-queue message", msg)
	},
}

var publishMessageCmd = &cobra.Command{
	Use:   "publish-message",
	Short: "Publish message to exchange",
	Run: func(cmd *cobra.Command, args []string) {
		exchangeName, _ := cmd.Flags().GetString("exchange-name")
		routingKey, _ := cmd.Flags().GetString("routing-key")
		message, _ := cmd.Flags().GetString("message")
		if exchangeName == "" {
			log.Fatal("please provide exchange name")
		}
		if routingKey == "" {
			log.Fatal("please provide routing key")
		}
		if message == "" {
			log.Fatal("please provide queue name")
		}
		msg, err := client.PublishMessage(exchangeName, routingKey, message)
		if err != nil {
			log.Fatal("error while publishing message", err.Error())
		}
		log.Println("publish-message message", msg)
	},
}

var retrieveMessagesCmd = &cobra.Command{
	Use:   "retrieve-messages",
	Short: "Retrieve messages from exchange",
	Run: func(cmd *cobra.Command, args []string) {
		queueName, _ := cmd.Flags().GetString("queue-name")
		count, _ := cmd.Flags().GetInt32("message-count")
		if queueName == "" {
			log.Fatal("please provide queue name")
		}
		if count == 0 {
			log.Fatal("please provide valid message count(>0)")
		}
		msg, err := client.RetrieveMessages(queueName, count)
		if err != nil {
			log.Fatal("error while retrieving queue messages", err.Error())
		}
		log.Println("retrieve-messages message", msg)
	},
}

var startConsumerCmd = &cobra.Command{
	Use:   "start-consumer",
	Short: "Start queue consumer",
	Run: func(cmd *cobra.Command, args []string) {
		queueName, _ := cmd.Flags().GetString("queue-name")
		if queueName == "" {
			log.Fatal("please provide queue name")
		}
		_, err := client.StartConsumer(queueName)
		if err != nil {
			log.Fatal("error while consuming queue messages", err.Error())
		}
	},
}

func init() {
	createExchangeCmd.Flags().StringP("exchange-name", "e", "", "Name of the exchange")

	createQueueCmd.Flags().StringP("queue-name", "q", "", "Name of the queue")

	bindQueueCmd.Flags().StringP("exchange-name", "e", "", "Name of the exchange")
	bindQueueCmd.Flags().StringP("queue-name", "q", "", "Name of the queue")
	bindQueueCmd.Flags().StringP("routing-key", "k", "", "Routing key for binding")

	publishMessageCmd.Flags().StringP("exchange-name", "e", "", "Name of the exchange")
	publishMessageCmd.Flags().StringP("routing-key", "k", "", "Routing key for binding")
	publishMessageCmd.Flags().StringP("message", "m", "", "Message string")

	startConsumerCmd.Flags().StringP("queue-name", "q", "", "Name of the queue")

	retrieveMessagesCmd.Flags().StringP("queue-name", "q", "", "Name of the queue")
	retrieveMessagesCmd.Flags().Int32P("message-count", "c", 0, "Message count to be retrieved")
}
