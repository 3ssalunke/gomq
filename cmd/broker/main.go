package main

import (
	"fmt"
	"log"
	"net"

	"github.com/3ssalunke/gomq/internal/config"
	_broker "github.com/3ssalunke/gomq/pkg/broker"
	"github.com/3ssalunke/gomq/pkg/protoc"
	"google.golang.org/grpc"
)

func main() {
	config := config.LoadConfig()

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", config.BrokerHost, config.BrokerPort))
	if err != nil {
		log.Fatalf(err.Error())
	}

	grpcServer := grpc.NewServer()

	broker := _broker.NewBroker(config)
	protoc.RegisterBrokerServiceServer(grpcServer, &_broker.BrokerServiceServer{Broker: broker})

	log.Printf("starting broker server at %s...\n", lis.Addr().String())

	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf(err.Error())
	}
}
