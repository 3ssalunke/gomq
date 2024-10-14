package config

import (
	"log"

	"github.com/spf13/viper"
)

type Config struct {
	PeerNodes  []string
	BrokerHost string
	BrokerPort string

	BrokerStoreDir    string
	BrokerStateDir    string
	BrokerMessagesDir string

	MessageAckTimeout        uint16
	MessageNackClearInterval uint16

	ConsumerConnectionRetries uint16
}

func LoadConfig() Config {
	viper.SetDefault("PEER_NODES", []string{})

	viper.SetDefault("BROKER_HOST", "localhost")
	viper.SetDefault("BROKER_PORT", "50051")

	viper.SetDefault("BROKER_STORE_DIR", "store")
	viper.SetDefault("BROKER_STATE_DIR", "state")
	viper.SetDefault("BROKER_MESSAGES_DIR", "messages")

	viper.SetDefault("MESSAGE_ACK_TIMEOUT", 60)
	viper.SetDefault("MESSAGE_NACK_CLEAR_INTERVAL", 30)

	viper.SetDefault("CONSUMER_CONNECTION_RETRIES", 3)

	viper.SetConfigFile(".env.broker")
	viper.SetConfigType("env")
	err := viper.ReadInConfig()
	if err != nil {
		log.Printf("error reading config file %v\n", err)
	}

	viper.AutomaticEnv()

	return Config{
		PeerNodes:                 viper.GetStringSlice("PEER_NODES"),
		BrokerHost:                viper.GetString("BROKER_HOST"),
		BrokerPort:                viper.GetString("BROKER_PORT"),
		BrokerStoreDir:            viper.GetString("BROKER_STORE_DIR"),
		BrokerStateDir:            viper.GetString("BROKER_STATE_DIR"),
		BrokerMessagesDir:         viper.GetString("BROKER_MESSAGES_DIR"),
		MessageAckTimeout:         viper.GetUint16("MESSAGE_ACK_TIMEOUT"),
		MessageNackClearInterval:  viper.GetUint16("MESSAGE_NACK_CLEAR_INTERVAL"),
		ConsumerConnectionRetries: viper.GetUint16("CONSUMER_CONNECTION_RETRIES"),
	}
}
