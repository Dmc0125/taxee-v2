package db

import (
	"encoding/json"
	"fmt"
	"taxee/pkg/assert"
	"time"

	"github.com/google/uuid"
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
		) as price,
		(
			case
				when mp.coingecko_id is not null then true
				else false
			end
		) as missing
	from
		coingecko_token ct
	left join
		pricepoint pp on
			pp.coingecko_id = ct.coingecko_id and pp.timestamp = $1
	left join
		missing_pricepoint mp on
			mp.coingecko_id = ct.coingecko_id and
			mp.timestamp_from <= $1 and
			mp.timestamp_to >= $1
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
		) as price,
		(
			case
				when mp.coingecko_id is not null then true
				else false
			end
		) as missing
	from 
		coingecko_token_data ct
	left join
		pricepoint pp on
			pp.coingecko_id = $1 and pp.timestamp = $2
	left join
		missing_pricepoint mp on
			mp.coingecko_id = $1 and
			mp.timestamp_from <= $2 and
			mp.timestamp_to >= $2 
	where
		ct.coingecko_id = $1
`

// InsertPricepoint
//
//	insert into pricepoint (
//		price, timestamp, coingecko_id
//	) values (
//		$1, $2, $3
//	) on conflict (timestamp, coingecko_id) do nothing
const InsertPricepoint string = `
	insert into pricepoint (
		price, timestamp, coingecko_id
	) values (
		$1, $2, $3
	) on conflict (timestamp, coingecko_id) do nothing
`

// SetMissingPricepoint
//
//	insert into missing_pricepoint (
//		coingecko_id, timestamp_from, timestamp_to
//	) values (
//		$1, $2, $3
//	) on conflict (coingecko_id) do update set
//		timestamp_from = case
//			when $2 < timestamp_from then $2
//			else timestamp_from
//		end,
//		timestamp_to = case
//			when $3 > timestamp_to then $3
//			else timestamp_to
//		end
const SetMissingPricepoint string = `
	insert into missing_pricepoint (
		coingecko_id, timestamp_from, timestamp_to
	) values (
		$1, $2, $3
	) on conflict (coingecko_id) do update set
		timestamp_from = case
			when $2 < missing_pricepoint.timestamp_from then $2
			else missing_pricepoint.timestamp_from
		end,
		timestamp_to = case
			when $3 > missing_pricepoint.timestamp_to then $3
			else missing_pricepoint.timestamp_to
		end
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

	Price  decimal.Decimal `json:"price"`
	Value  decimal.Decimal `json:"value"`
	Profit decimal.Decimal `json:"profit"`

	MissingAmount decimal.Decimal `json:"missingAmount"`
}

type EventSwapTransfer struct {
	Account     string          `json:"account"`
	Token       string          `json:"token"`
	Amount      decimal.Decimal `json:"amount"`
	TokenSource uint16          `json:"tokenSource"`

	Price  decimal.Decimal `json:"price"`
	Value  decimal.Decimal `json:"value"`
	Profit decimal.Decimal `json:"profit"`

	MissingAmount decimal.Decimal `json:"missingAmount"`
}

type EventSwap struct {
	Wallet   string               `json:"wallet"`
	Outgoing []*EventSwapTransfer `json:"outgoing"`
	Incoming []*EventSwapTransfer `json:"incoming"`
}

type EventType int

const (
	EventTypeTransfer EventType = iota
	EventTypeCloseAccount
	EventTypeMint
	EventTypeBurn

	EventTypeSwap

	EventTypeAddLiquidity
	EventTypeRemoveLiquidity

	EventTypeStake
)

type Event struct {
	Id              uuid.UUID
	Timestamp       time.Time
	Network         Network
	UiAppName       string
	UiMethodName    string
	Type            EventType
	Data            any
	PrecedingEvents []uuid.UUID
}

func (event *Event) UnmarshalData(src []byte) error {
	var data any
	switch event.Type {
	case EventTypeTransfer, EventTypeCloseAccount, EventTypeMint, EventTypeBurn:
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
