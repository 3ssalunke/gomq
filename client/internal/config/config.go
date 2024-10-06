package config

import (
	"log"

	"github.com/spf13/viper"
)

type Config struct {
	BrokerAddr string

	MaxStreamRetries          uint16
	MaxConnectionRetries      uint16
	StreamBackoffWaittime     uint16
	ConnectionBackoffWaittime uint16
}

func LoadConfig() Config {
	viper.SetDefault("BROKER_ADDR", "localhost")
	viper.SetDefault("MAX_STREAM_RETRIES", "3")
	viper.SetDefault("MAX_CONNECTION_RETRIES", "3")
	viper.SetDefault("STREAM_BACKOFF_WAITTIME", "5")
	viper.SetDefault("CONNECTION_BACKOFF_WAITTIME", "5")

	viper.SetConfigFile(".env.client")
	viper.SetConfigType("env")

	err := viper.ReadInConfig()
	if err != nil {
		log.Printf("error reading config file %v\n", err)
	}

	return Config{
		BrokerAddr:                viper.GetString("BROKER_ADDR"),
		MaxStreamRetries:          viper.GetUint16("MAX_STREAM_RETRIES"),
		MaxConnectionRetries:      viper.GetUint16("MAX_CONNECTION_RETRIES"),
		StreamBackoffWaittime:     viper.GetUint16("STREAM_BACKOFF_WAITTIME"),
		ConnectionBackoffWaittime: viper.GetUint16("CONNECTION_BACKOFF_WAITTIME"),
	}
}
