package main

import (
	"fmt"
	"log"
	"net"

	"github.com/3ssalunke/gomq/broker/internal/config"
	broker "github.com/3ssalunke/gomq/broker/pkg"
	"github.com/3ssalunke/gomq/shared/pkg/protoc"
	"google.golang.org/grpc"
)

func main() {
	config := config.LoadConfig()

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", config.BrokerHost, config.BrokerPort))
	if err != nil {
		log.Fatalf(err.Error())
	}

	grpcServer := grpc.NewServer()

	_broker := broker.NewBroker(config)
	protoc.RegisterBrokerServiceServer(grpcServer, &broker.BrokerServiceServer{Broker: _broker})

	log.Printf("starting broker server at %s...\n", lis.Addr().String())

	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf(err.Error())
	}
}
