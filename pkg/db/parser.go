package db

import (
	"encoding/json"
	"fmt"
	"taxee/pkg/assert"
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

type EventTransferDirection uint8

const (
	EventTransferIncoming EventTransferDirection = iota
	EventTransferOutgoing
	EventTransferInternal
)

type EventTransfer struct {
	Direction   EventTransferDirection `json:"direction"`
	FromWallet  string                 `json:"fromWallet"`
	ToWallet    string                 `json:"toWallet"`
	FromAccount string                 `json:"fromaccount"`
	ToAccount   string                 `json:"toAccount"`
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
	Wallet   string               `json:"wallet"`
	Outgoing []*EventSwapTransfer `json:"outgoing"`
	Incoming []*EventSwapTransfer `json:"incoming"`
}

type EventType int

const (
	EventTypeTransfer EventType = iota
	EventTypeMint
	EventTypeBurn

	EventTypeSwap

	EventTypeAddLiquidity
	EventTypeRemoveLiquidity

	EventTypeStake
)

type Event struct {
	TxId         string
	Idx          int
	IxIdx        int32
	Timestamp    time.Time
	Network      Network
	UiAppName    string
	UiMethodName string
	Type         EventType
	Data         any
}

func (event *Event) UnmarshalData(src []byte) error {
	var data any
	switch event.Type {
	case EventTypeTransfer, EventTypeMint, EventTypeBurn:
		data = new(EventTransfer)
	case EventTypeSwap, EventTypeAddLiquidity, EventTypeRemoveLiquidity:
		data = new(EventSwap)
	}

	if err := json.Unmarshal(src, data); err != nil {
		return fmt.Errorf("unable to unmarshal %d event data: %w", event.Type, err)
	}

	event.Data = data
	return nil
}

type SyncRequestType uint8
type SyncRequestStatus uint8

const (
	// fetch + parse txs and events
	SyncRequestFetch SyncRequestType = iota
	// parse txs and events
	SyncRequestParseTxs
	// parse events
	SyncRequestParseEvents
)

const (
	SyncRequestQueued SyncRequestStatus = iota
	SyncRequestProcessing
)

func (s *SyncRequestStatus) String() string {
	switch *s {
	case SyncRequestQueued:
		return "In queue"
	case SyncRequestProcessing:
		return "Processing"
	default:
		assert.True(false, "invalid status: %d", *s)
		return ""
	}
}
