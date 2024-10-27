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
	"google.golang.org/grpc/reflection"
)

func main() {
	config := config.LoadConfig()

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", config.BrokerHost, config.BrokerPort))
	if err != nil {
		log.Fatalf(err.Error())
	}

	broker := broker.NewBroker(config)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(middleware.ApiKeyAuthInterceptor(broker.Auth)),
	)

	protoc.RegisterBrokerServiceServer(grpcServer, &server.BrokerServiceServer{Broker: broker})
	protoc.RegisterClusterSyncServer(grpcServer, &server.ClusterSyncServer{Broker: broker})

	reflection.Register(grpcServer)

	log.Printf("starting broker server at %s...\n", lis.Addr().String())

	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf(err.Error())
	}
}
