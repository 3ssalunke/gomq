package main

import (
	"fmt"
	"net"

	"github.com/3ssalunke/gomq/pkg"
	"github.com/3ssalunke/gomq/pkg/protoc"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		fmt.Println(err.Error())
	}
	grpcServer := grpc.NewServer()
	broker := pkg.NewBroker()
	protoc.RegisterBrokerServiceServer(grpcServer, &pkg.BrokerServiceServer{Broker: broker})
	grpcServer.Serve(lis)
}
