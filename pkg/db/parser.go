package db

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// GetPricepointByNetworkAndTokenAddress
//
//	select
//		ct.coingecko_id,
//		(
//			case
//				when pp.coingecko_id is not null then pp.price
//				else ''
//			end
//		) as price
//	from
//		coingecko_token ct
//	left join
//		pricepoint pp on
//			pp.coingecko_id = ct.coingecko_id and pp.timestamp = $1
//	where
//		ct.network = $2 and ct.address = $3
const GetPricepointByNetworkAndTokenAddress string = `
	select
		ct.coingecko_id,
		(
			case
				when pp.coingecko_id is not null then pp.price
				else ''
			end
		) as price
	from
		coingecko_token ct
	left join
		pricepoint pp on
			pp.coingecko_id = ct.coingecko_id and pp.timestamp = $1
	where
		ct.network = $2 and ct.address = $3
`

// GetPricepointByCoingeckoId
//
//	select
//		ct.coingecko_id,
//		(
//			case
//				when pp.coingecko_id is not null then pp.price
//				else ''
//			end
//		) as price
//	from
//		coingecko_token_data ct
//	left join
//		pricepoint pp on
//			pp.coingecko_id = $1 and pp.timestamp = $2
//	where
//		ct.coingecko_id = $1
const GetPricepointByCoingeckoId string = `
	select 
		ct.coingecko_id,
		(
			case
				when pp.coingecko_id is not null then pp.price
				else ''
			end
		) as price
	from 
		coingecko_token_data ct
	left join
		pricepoint pp on
			pp.coingecko_id = $1 and pp.timestamp = $2
	where
		ct.coingecko_id = $1
`

type EventTransferInternal struct {
	// wallets corresponding to accounts
	FromWallet string `json:"fromWallet"`
	ToWallet   string `json:"toWallet"`

	FromAccount string          `json:"fromAccount"`
	ToAccount   string          `json:"toAccount"`
	Token       string          `json:"token"`
	Amount      decimal.Decimal `json:"amount"`
	TokenSource uint16          `json:"tokenSource"`
	Price       decimal.Decimal `json:"price"`
	Value       decimal.Decimal `json:"value"`
	Profit      decimal.Decimal `json:"profit"`
}

type EventTransferDirection uint8

const (
	EventTransferIncoming EventTransferDirection = iota
	EventTransferOutgoing
)

type EventTransfer struct {
	Direction   EventTransferDirection `json:"direction"`
	Wallet      string                 `json:"wallet"`
	Account     string                 `json:"account"`
	Token       string                 `json:"token"`
	Amount      decimal.Decimal        `json:"amount"`
	TokenSource uint16                 `json:"tokenSource"`
	Price       decimal.Decimal        `json:"price"`
	Value       decimal.Decimal        `json:"value"`
	Profit      decimal.Decimal        `json:"profit"`
}

type EventSwapTransfer struct {
	Account     string          `json:"account"`
	Token       string          `json:"token"`
	Amount      decimal.Decimal `json:"amount"`
	TokenSource uint16          `json:"tokenSource"`
	Price       decimal.Decimal `json:"price"`
	Value       decimal.Decimal `json:"value"`
	Profit      decimal.Decimal `json:"profit"`
}

type EventSwap struct {
	Wallet   string              `json:"wallet"`
	Outgoing []*EventSwapTransfer `json:"outgoing"`
	Incoming []*EventSwapTransfer `json:"incoming"`
}

type EventType string

const (
	EventTypeTransfer         EventType = "transfer"
	EventTypeTransferInternal EventType = "transfer_internal"
	EventTypeMint             EventType = "mint"
	EventTypeBurn             EventType = "burn"
	EventTypeSwap             EventType = "swap"
)

type Event struct {
	TxId         string
	IxIdx        int32
	Idx          int32
	Timestamp    time.Time
	Network      Network
	UiAppName    string
	UiMethodName string
	Type         EventType
	Data         any
}

func (event *Event) UnmarshalData(src []byte) error {
	switch event.Type {
	case EventTypeTransferInternal:
		data := new(EventTransferInternal)
		if err := json.Unmarshal(src, data); err != nil {
			return fmt.Errorf("unable to unmarshal %s event data: %w", event.Type, err)
		}
		event.Data = data
	case EventTypeTransfer, EventTypeMint, EventTypeBurn:
		data := new(EventTransfer)
		if err := json.Unmarshal(src, data); err != nil {
			return fmt.Errorf("unable to unmarshal %s event data: %w", event.Type, err)
		}
		event.Data = data
	case EventTypeSwap:
		data := new(EventSwap)
		if err := json.Unmarshal(src, data); err != nil {
			return fmt.Errorf("unable to unmarshal %s event data: %w", event.Type, err)
		}
		event.Data = data
	}

	return nil
}
