package db

import (
	"database/sql/driver"
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
//		) as price,
//		(
//			case
//				when mp.coingecko_id is not null then true
//				else false
//			end
//		) as missing
//	from
//		coingecko_token ct
//	left join
//		pricepoint pp on
//			pp.coingecko_id = ct.coingecko_id and pp.timestamp = $1
//	left join
//		missing_pricepoint mp on
//			mp.coingecko_id = ct.coingecko_id and
//			mp.timestamp_from <= $1 and
//			mp.timestamp_to >= $1
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
//		) as price,
//		(
//			case
//				when mp.coingecko_id is not null then true
//				else false
//			end
//		) as missing
//	from
//		coingecko_token_data ct
//	left join
//		pricepoint pp on
//			pp.coingecko_id = $1 and pp.timestamp = $2
//	left join
//		missing_pricepoint mp on
//			mp.coingecko_id = $1 and
//			mp.timestamp_from <= $2 and
//			mp.timestamp_to >= $2
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

type ParserStatus string

const (
	ParserStatusUninitialized ParserStatus = "uninitialized"

	ParserStatusPTQueued     ParserStatus = "pt_queued"
	ParserStatusPTInProgress ParserStatus = "pt_in_progress"
	ParserStatusPTError      ParserStatus = "pt_error"

	ParserStatusPEQueued     ParserStatus = "pe_queued"
	ParserStatusPEInProgress ParserStatus = "pe_in_progress"
	ParserStatusPEError      ParserStatus = "pe_error"

	ParserStatusSuccess ParserStatus = "success"
	ParserStatusReset   ParserStatus = "reset"
)

func (dst *ParserStatus) Scan(src any) error {
	if src == nil {
		*dst = ParserStatusUninitialized
		return nil
	}

	n, ok := src.(string)
	if !ok {
		return fmt.Errorf("invalid status type: %T", src)
	}

	// TODO: validation
	*dst = ParserStatus(n)
	return nil
}

func (src ParserStatus) Value() (driver.Value, error) {
	// TODO: validation
	switch src {
	case ParserStatusUninitialized:
		return nil, nil
	default:
		return string(src), nil
	}
}

type EventTransferDirection uint8

const (
	EventTransferIncoming EventTransferDirection = iota
	EventTransferOutgoing
	EventTransferInternal
)

// compares two directions, outgoing is always < incoming
//
// should ne be used to compare internal
func (d1 *EventTransferDirection) Cmp(d2 EventTransferDirection) int {
	if *d1 == d2 {
		return 0
	}
	if *d1 == EventTransferOutgoing && d2 == EventTransferIncoming {
		return -1
	}
	return 1
}

type EventTransferSource struct {
	TransferId uuid.UUID       `json:"transferId"`
	UsedAmount decimal.Decimal `json:"usedAmount"`
}

type EventTransfer struct {
	Id uuid.UUID `json:"id"`

	Direction   EventTransferDirection `json:"direction"`
	FromWallet  string                 `json:"fromWallet"`
	FromAccount string                 `json:"fromaccount"`
	ToWallet    string                 `json:"toWallet"`
	ToAccount   string                 `json:"toAccount"`

	Token       string          `json:"token"`
	Amount      decimal.Decimal `json:"amount"`
	TokenSource uint16          `json:"tokenSource"`

	Price         decimal.Decimal `json:"price"`
	Value         decimal.Decimal `json:"value"`
	Profit        decimal.Decimal `json:"profit"`
	MissingAmount decimal.Decimal `json:"missingAmount"`

	Sources []*EventTransferSource `json:"sources"`
}

type EventType int

const (
	EventTypeTransfer EventType = iota
	EventTypeCloseAccount
	EventTypeMint
	EventTypeBurn

	EventTypeBorrowRepay
	// EventTypeStake

	EventTypeSwapBr

	EventTypeSwap
	EventTypeAddLiquidity
	EventTypeRemoveLiquidity
)

type Event struct {
	Id        uuid.UUID
	Timestamp time.Time
	Network   Network

	App       string
	Method    string
	Type      EventType
	Transfers []*EventTransfer
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
