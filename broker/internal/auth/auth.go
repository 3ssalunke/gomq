package auth

import (
	"errors"
	"fmt"
)

type Role int

const (
	Admin Role = iota
	Producer
	Consumer
)

func (r Role) String() string {
	switch r {
	case Admin:
		return "Admin"
	case Producer:
		return "Producer"
	case Consumer:
		return "Consumer"
	default:
		return "unknown"
	}
}

func NewRole(role string) (Role, error) {
	switch role {
	case "Admin":
		return Admin, nil
	case "Producer":
		return Producer, nil
	case "Consumer":
		return Consumer, nil
	default:
		return 0, fmt.Errorf("invalid role: %s", role)
	}
}

type User struct {
	Name   string
	Role   Role
	ApiKey string
}

var users = map[string]User{}

func ValidateApiKey(apiKey string) (*User, error) {
	if client, exists := users[apiKey]; exists {
		return &client, nil
	}

	return nil, errors.New("invalid api key")
}
