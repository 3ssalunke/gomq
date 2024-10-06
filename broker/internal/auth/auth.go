package auth

import "errors"

type Client struct {
	APIKey string
	Name   string
}

var clients = map[string]Client{
	"admin": {
		APIKey: "admin-api-key",
		Name:   "Admin",
	},
}

func ValidateApiKey(apiKey string) (*Client, error) {
	if client, exists := clients[apiKey]; exists {
		return &client, nil
	}

	return nil, errors.New("invalid api key")
}
