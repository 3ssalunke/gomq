package auth

import (
	"errors"
	"fmt"

	"github.com/3ssalunke/gomq/shared/util"
	"github.com/google/uuid"
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
	case "admin":
		return Admin, nil
	case "producer":
		return Producer, nil
	case "consumer":
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

type Auth struct {
	Admin *User
	Users map[string]*User
}

func NewAuth() *Auth {
	return &Auth{
		Admin: nil,
		Users: map[string]*User{},
	}
}

func (au *Auth) ValidateApiKey(apiKey string) (*User, error) {
	if au.Admin != nil && apiKey == au.Admin.ApiKey {
		return au.Admin, nil
	} else if user, exists := au.Users[apiKey]; exists {
		return user, nil

	}

	return nil, errors.New("invalid api key")
}

func (au *Auth) CreateAdmin(username string) (string, error) {
	if au.Admin != nil {
		return "", errors.New("admin role already exists")
	}

	apiKey := uuid.New().String()
	au.Admin = &User{
		Name:   username,
		Role:   Admin,
		ApiKey: apiKey,
	}

	return apiKey, nil
}

func (au *Auth) RemoveAdmin() error {
	if au.Admin == nil {
		return errors.New("admin does not exist")
	}

	au.Admin = nil

	return nil
}

func (au *Auth) CreateUser(username, role string) (string, error) {
	parsedRole, err := NewRole(role)
	if err != nil || parsedRole == Admin {
		return "", fmt.Errorf("role %s is invalid role", role)
	}

	for _, subject := range au.Users {
		if subject.Name == username && subject.Role == parsedRole {
			return "", fmt.Errorf("user %s with role %s already exists", username, role)
		}
	}

	apiKey := uuid.New().String()
	au.Users[apiKey] = &User{
		Name:   username,
		Role:   parsedRole,
		ApiKey: apiKey,
	}

	return apiKey, nil
}

func (au *Auth) RemoveUserApiKey(apiKey string) (*User, error) {
	if !util.MapContains(au.Users, apiKey) {
		return nil, fmt.Errorf("api key %s does not exist", apiKey)
	}

	user := au.Users[apiKey]

	delete(au.Users, apiKey)

	return user, nil
}
