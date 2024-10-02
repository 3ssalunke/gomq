package client

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/3ssalunke/gomq/pkg/protoc"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func createClient() (*grpc.ClientConn, protoc.BrokerServiceClient, error) {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	return conn, protoc.NewBrokerServiceClient(conn), nil
}

func CreateExchange(name, extype, schema string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	res, err := client.CreateExchange(context.TODO(), &protoc.Exchange{Name: name, Type: extype, Schema: schema})
	if err != nil {
		return "", err
	}
	return res.Message, nil
}

func RemoveExchange(name string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	res, err := client.RemoveExchange(context.TODO(), &protoc.RemoveExchangeRequest{ExchangeName: name})
	if err != nil {
		return "", err
	}
	return res.Message, nil
}

func CreateQueue(name string, dlq bool, maxRetries int8) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	queue := &protoc.Queue{
		Name:       name,
		Dlq:        dlq,
		MaxRetries: int32(maxRetries),
	}

	res, err := client.CreateQueue(context.TODO(), queue)
	if err != nil {
		return "", err
	}
	return res.Message, nil
}

func RemoveQueue(exchangeName, queueName string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	res, err := client.RemoveQueue(context.TODO(), &protoc.RemoveQueueRequest{ExchangeName: exchangeName, QueueName: queueName})
	if err != nil {
		return "", err
	}
	return res.Message, nil
}

func BindQueue(exchangeName, queueName, routingKey string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	res, err := client.BindQueue(context.TODO(), &protoc.Binding{Exchange: exchangeName, Queue: queueName, RoutingKey: routingKey})
	if err != nil {
		return "", err
	}
	return res.Message, nil
}

func PublishMessage(exchangeName, routingKey string, message []byte) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	encodedMessage := &protoc.Message{
		Id:        uuid.New().String(),
		Payload:   message,
		Timestamp: time.Now().UnixMilli(),
	}
	res, err := client.PublishMessage(context.TODO(), &protoc.PublishMessageRequest{Exchange: exchangeName, RoutingKey: routingKey, Message: encodedMessage})
	if err != nil {
		return "", err
	}
	return res.Message, nil
}

func RetrieveMessages(queueName string, count int32) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	res, err := client.RetrieveMessages(context.TODO(), &protoc.RetrieveMessagesRequest{Queue: queueName, Count: count})
	if err != nil {
		return "", err
	}
	for _, msg := range res.Messages {
		log.Printf("retrieved message payload for id %s is %s", msg.Id, msg.Payload)
	}
	return res.Message, nil
}

func StartConsumer(queueName string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.ConsumeMessages(ctx, &protoc.Queue{Name: queueName})
	if err != nil {
		return "", err
	}

	streamRetries := 0
	connectionRetries := 0
	streamBackoffWaitTime := 5
	connectionBackoffWaitTime := 5

	for {
		var msg *protoc.Message

		if stream != nil {
			msg, err = stream.Recv()
		}

		if err != nil {
			if status.Code(err) == codes.Unavailable {
				if connectionRetries > 3 {
					return "", fmt.Errorf("broker crashed")
				}

				connectionRetries++
				time.Sleep(time.Second * time.Duration(connectionBackoffWaitTime))
				connectionBackoffWaitTime = connectionBackoffWaitTime * 2

				stream, err = client.ConsumeMessages(ctx, &protoc.Queue{Name: queueName})
				continue

			}

			if streamRetries > 3 {
				return "", fmt.Errorf("broker stream crashed")
			}
			streamRetries++
			time.Sleep(time.Second * time.Duration(streamBackoffWaitTime))
			streamBackoffWaitTime = streamBackoffWaitTime * 2
			continue
		}

		if streamRetries > 0 {
			streamRetries = 0
		}
		if connectionRetries > 0 {
			connectionRetries = 0
		}

		client.MessageAcknowledge(ctx, &protoc.MessageAckRequest{Queue: queueName, MesssageId: msg.Id})
		log.Println(msg.Payload)
	}
}

func RedriveDlqMessages(queueName string) (string, error) {
	conn, client, err := createClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	res, err := client.RedriveDlqMessages(context.TODO(), &protoc.Queue{Name: queueName})
	if err != nil {
		return "", err
	}
	log.Println(res.Message)
	return res.Message, nil
}
