package server

import (
	"context"
	"fmt"
	"log"

	"github.com/3ssalunke/gomq/broker/internal/auth"
	"github.com/3ssalunke/gomq/broker/internal/broker"
	"github.com/3ssalunke/gomq/broker/internal/storage"
	"github.com/3ssalunke/gomq/shared/pkg/protoc"
	"github.com/3ssalunke/gomq/shared/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ClusterSyncServer struct {
	protoc.UnimplementedClusterSyncServer
	Broker *broker.Broker
}

func (s *ClusterSyncServer) BroadCastMessageToPeer(ctx context.Context, req *protoc.BroadCastMessageToPeerRequest) (*protoc.SyncClusterStateResponse, error) {
	if s.Broker.Config.IsLeader {
		log.Println("invalid rpc call, broker node is not slave")
		return nil, status.Errorf(codes.PermissionDenied, "broker node is not slave")
	}

	message := &storage.Message{
		ID:        req.Message.Id,
		Payload:   req.Message.Payload,
		Timestamp: req.Message.Timestamp,
	}

	if err := s.Broker.ProcessBroadcastedMessage(message); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to save message")
	}

	return &protoc.SyncClusterStateResponse{Status: true, Message: "message saved succcesfully"}, nil
}

func (s *ClusterSyncServer) SyncCluster(ctx context.Context, req *protoc.SyncClusterStateRequest) (*protoc.SyncClusterStateResponse, error) {
	if s.Broker.Config.IsLeader {
		log.Println("invalid rpc call, broker node is not slave")
		return nil, status.Errorf(codes.PermissionDenied, "broker node is not slave")
	}

	_authStore := req.AuthStore
	_exchanges := req.Exchanges
	_schemaRegistry := req.SchemaRegistry
	_queues := req.Queues
	_queueConfigs := req.QueueConfigs
	_bindings := req.Bindings

	authStore := &auth.Auth{}
	exchanges := make(map[string]storage.ExchangeType)
	schemaRegistry := make(map[string]string)
	queues := make(map[string][]string)
	queueConfigs := make(map[string]storage.QueueConfig)
	bindings := make(map[string]map[string][]string)

	admin := &auth.User{}
	admin.ApiKey = _authStore.Admin.ApiKey
	admin.Name = _authStore.Admin.Name
	admin.Role = auth.Role(_authStore.Admin.Role)
	authStore.Admin = admin

	users := make(map[string]*auth.User)
	for _key, _user := range _authStore.Users {
		user := &auth.User{
			ApiKey: _user.ApiKey,
			Name:   _user.Name,
			Role:   auth.Role(_user.Role),
		}
		users[_key] = user
	}
	authStore.Users = users

	for _exchangeName, _exchangeType := range _exchanges {
		exchanges[_exchangeName] = storage.ExchangeType(_exchangeType)
	}

	for _exchangeName, _exchangeSchema := range _schemaRegistry {
		schemaRegistry[_exchangeName] = _exchangeSchema
	}

	for _queueName, msgIDs := range _queues {
		queues[_queueName] = msgIDs.Elements
		queueConfigs[_queueName] = storage.QueueConfig{
			DLQ:        _queueConfigs[_queueName].Dlq,
			MaxRetries: int8(_queueConfigs[_queueName].MaxRetries),
		}
	}

	for _exchangeName, _bindings := range _bindings {
		for _route, _queues := range _bindings.QueueBindings {
			if !util.MapContains(bindings, _exchangeName) {
				bindings[_exchangeName] = map[string][]string{}
			}
			bindings[_exchangeName][_route] = _queues.Elements
		}
	}

	if err := s.Broker.SyncWithMasterState(authStore, exchanges, schemaRegistry, queues, queueConfigs, bindings); err != nil {
		return nil, status.Errorf(codes.Unknown, fmt.Sprintf("error while syncing with master %s", err.Error()))
	}

	return nil, nil
}
