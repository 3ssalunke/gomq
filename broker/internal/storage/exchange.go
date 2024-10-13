package storage

import "fmt"

type ExchangeType int

const (
	Direct ExchangeType = iota
	Fanout
)

func (r ExchangeType) String() string {
	switch r {
	case Direct:
		return "direct"
	case Fanout:
		return "fanout"
	default:
		return "unknown"
	}
}

func NewExchangeType(eType string) (ExchangeType, error) {
	switch eType {
	case "direct":
		return Direct, nil
	case "fanout":
		return Fanout, nil
	default:
		return 0, fmt.Errorf("invalid exchange type: %s", eType)
	}
}
