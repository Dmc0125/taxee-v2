package db

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"taxee/pkg/assert"
	"time"

	"github.com/jackc/pgx/v5"
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

type EventData interface {
	SetPrice(decimal.Decimal)
	SetAmountDecimals(decimals uint8)
}

type EventTransferInternal struct {
	FromAccount string          `json:"fromAccount"`
	ToAccount   string          `json:"toAccount"`
	Token       string          `json:"token"`
	Amount      decimal.Decimal `json:"amount"`
	TokenSource uint16          `json:"-"`
	Price       decimal.Decimal `json:"price"`
	Value       decimal.Decimal `json:"value"`
	Profit      decimal.Decimal `json:"profit"`
}

var _ EventData = (*EventTransferInternal)(nil)

func (internal *EventTransferInternal) SetPrice(price decimal.Decimal) {
	internal.Price = price
	internal.Value = price.Mul(internal.Amount)
}

func (t *EventTransferInternal) SetAmountDecimals(decimals uint8) {
	t.Amount = decimal.NewFromBigInt(
		t.Amount.BigInt(),
		-int32(decimals),
	)
}

type EventTransferDirection uint8

const (
	EventTransferIncoming EventTransferDirection = iota
	EventTransferOutgoing
)

type EventTransfer struct {
	Direction   EventTransferDirection `json:"direction"`
	Account     string                 `json:"account"`
	Token       string                 `json:"token"`
	Amount      decimal.Decimal        `json:"amount"`
	TokenSource uint16                 `json:"-"`
	Price       decimal.Decimal        `json:"price"`
	Value       decimal.Decimal        `json:"value"`
	Profit      decimal.Decimal        `json:"profit"`
}

var _ EventData = (*EventTransfer)(nil)

func (transfer *EventTransfer) SetPrice(price decimal.Decimal) {
	transfer.Price = price
	transfer.Value = price.Mul(transfer.Amount)
}

func (t *EventTransfer) SetAmountDecimals(decimals uint8) {
	t.Amount = decimal.NewFromBigInt(
		t.Amount.BigInt(),
		-int32(decimals),
	)
}

type EventType string

const (
	EventTypeTransfer         EventType = "transfer"
	EventTypeTransferInternal EventType = "transfer_internal"
	EventTypeMint             EventType = "mint"
	EventTypeBurn             EventType = "burn"
)

type Event struct {
	UiAppName    string
	UiMethodName string
	Timestamp    time.Time
	Network      Network
	TxId         string
	IxIdx        int32

	Type EventType
	Data EventData
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
	}

	return nil
}

func EnqueueInsertEvent(
	batch *pgx.Batch,
	userAccountId,
	idx int32,
	event *Event,
) (*pgx.QueuedQuery, error) {
	const q = `
		insert into event (
			user_account_id, tx_id, network, ix_idx, idx, timestamp,
			ui_app_name, ui_method_name, type,
			data
		) values (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
	`

	data, err := json.Marshal(event.Data)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal event data: %w", err)
	}

	return batch.Queue(
		q,
		userAccountId, event.TxId, event.Network, event.IxIdx, idx, event.Timestamp,
		event.UiAppName, event.UiMethodName, event.Type, data,
	), nil
}

type ErrOrigin string

const (
	ErrOriginPreprocess ErrOrigin = "preparse"
	ErrOriginProcess    ErrOrigin = "parse"
)

type ParserErrorMissingAccount struct {
	AccountAddress string `json:"accountAddress"`
}

func (e1 *ParserErrorMissingAccount) IsEq(other any) bool {
	e2, ok := other.(*ParserErrorMissingAccount)
	if !ok {
		return false
	}
	return e1.AccountAddress == e2.AccountAddress
}

func (e1 *ParserErrorMissingAccount) Address() string {
	return e1.AccountAddress
}

// NOTE: balance validation after preprocess / process
type ParserErrorAccountBalanceMismatch struct {
	AccountAddress string
	Token          string
	Expected       decimal.Decimal
	Had            decimal.Decimal
}

func (e1 *ParserErrorAccountBalanceMismatch) IsEq(other any) bool {
	e2, ok := other.(*ParserErrorAccountBalanceMismatch)
	if !ok {
		return false
	}
	return e1.AccountAddress == e2.AccountAddress &&
		e1.Token == e2.Token &&
		e1.Expected.Equal(e2.Expected) &&
		e1.Had.Equal(e2.Had)
}

func (e1 *ParserErrorAccountBalanceMismatch) Address() string {
	return e1.AccountAddress
}

type ParserErrorMissingPrice struct {
	Token     string    `json:"token"`
	Timestamp time.Time `json:"-"`
}

type ParserErrorInsufficientBalance struct {
	AccountAddress string
	Token          string
	AmountMissing  decimal.Decimal
}

type ParserErrorType uint8

const (
	ParserErrorTypeMissingAccount ParserErrorType = iota
	ParserErrorTypeAccountBalanceMismatch
	ParserErrorTypeMissingPrice
	ParserErrorTypeInsufficientBalance
)

func NewParserErrorTypeFromString(s string) (ParserErrorType, error) {
	switch s {
	case "missing_account":
		return ParserErrorTypeMissingAccount, nil
	case "account_balance_mismatch":
		return ParserErrorTypeAccountBalanceMismatch, nil
	case "missing_price":
		return ParserErrorTypeMissingPrice, nil
	case "insufficient_balance":
		return ParserErrorTypeInsufficientBalance, nil
	default:
		return 0, fmt.Errorf("invalid parser error type: %s", s)
	}
}

func (dst *ParserErrorType) Scan(src any) error {
	if src == nil {
		return nil
	}

	t, ok := src.(string)
	if !ok {
		return fmt.Errorf("invalid parser error type: %T", src)
	}

	var err error
	*dst, err = NewParserErrorTypeFromString(t)

	return err
}

func (e ParserErrorType) Value() (driver.Value, error) {
	switch e {
	case ParserErrorTypeMissingAccount:
		return "missing_account", nil
	case ParserErrorTypeAccountBalanceMismatch:
		return "account_balance_mismatch", nil
	case ParserErrorTypeMissingPrice:
		return "missing_price", nil
	case ParserErrorTypeInsufficientBalance:
		return "insufficient_balance", nil
	default:
		assert.True(false, "invalid parser error type: %d", e)
		return nil, nil
	}
}

type ParserError struct {
	TxId  string
	IxIdx uint32

	Type ParserErrorType
	Data any
}

// InsertParserError
//
//	insert into
//		parser_err (
//			user_account_id, tx_id, ix_idx, origin, type, data
//		)
//	values (
//		$1, $2, $3, $4, $5, $6
//	)
const InsertParserError string = `
	insert into
		parser_err (
			user_account_id, tx_id, ix_idx, origin, type, data
		)
	values (
		$1, $2, $3, $4, $5, $6
	)
`
