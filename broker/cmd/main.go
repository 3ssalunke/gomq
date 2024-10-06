package main

import (
	"fmt"
	"log"
	"net"

	"github.com/3ssalunke/gomq/broker/internal/broker"
	"github.com/3ssalunke/gomq/broker/internal/config"
	"github.com/3ssalunke/gomq/broker/pkg/middleware"
	"github.com/3ssalunke/gomq/broker/pkg/server"
	"github.com/3ssalunke/gomq/shared/pkg/protoc"
	"google.golang.org/grpc"
)

func main() {
	config := config.LoadConfig()

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", config.BrokerHost, config.BrokerPort))
	if err != nil {
		log.Fatalf(err.Error())
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(middleware.ApiKeyAuthInterceptor),
	)

	broker := broker.NewBroker(config)
	protoc.RegisterBrokerServiceServer(grpcServer, &server.BrokerServiceServer{Broker: broker})

	log.Printf("starting broker server at %s...\n", lis.Addr().String())

	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf(err.Error())
	}
}
