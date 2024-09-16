package main

import (
	"log"
	"net"

	_broker "github.com/3ssalunke/gomq/pkg/broker"
	"github.com/3ssalunke/gomq/pkg/protoc"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf(err.Error())
	}
	grpcServer := grpc.NewServer()
	broker := _broker.NewBroker()
	protoc.RegisterBrokerServiceServer(grpcServer, &_broker.BrokerServiceServer{Broker: broker})
	log.Println("starting broker server...")
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf(err.Error())
	}
}
