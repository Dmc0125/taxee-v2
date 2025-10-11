package db

import (
	"encoding/json"
	"fmt"
	"taxee/pkg/assert"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

func EnqueueInsertCoingeckoTokenData(
	batch *pgx.Batch,
	coingeckoId, symbol, name string,
) *pgx.QueuedQuery {
	const q = `
		insert into coingecko_token_data (
			coingecko_id, symbol, name
		) values (
			$1, $2, $3
		) on conflict (coingecko_id) do nothing
	`
	return batch.Queue(
		q, coingeckoId, symbol, name,
	)
}

func EnqueueInsertCoingeckoToken(
	batch *pgx.Batch,
	coingeckoId string,
	network Network,
	address string,
) *pgx.QueuedQuery {
	const q = `
		insert into coingecko_token (
			coingecko_id, network, address
		) values (
			$1, $2, $3
		) on conflict (network, address) do nothing
	`
	return batch.Queue(q, coingeckoId, network, address)
}

func EnqueueInsertPricepoint(
	batch *pgx.Batch,
	price decimal.Decimal,
	timestamp time.Time,
	coingeckoId string,
) *pgx.QueuedQuery {
	const q = `
		insert into pricepoint (
			price, timestamp, coingecko_id
		) values (
			$1, $2, $3
		) on conflict (timestamp, coingecko_id) do nothing
	`
	return batch.Queue(q, price, timestamp, coingeckoId)
}

func EnqueueGetPricepoint(
	batch *pgx.Batch,
	network Network,
	address string,
	timestamp time.Time,
) *pgx.QueuedQuery {
	const qGetPricepoint = `
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
	return batch.Queue(qGetPricepoint, timestamp, network, address)
}

func QueueScanPricepoint(
	row pgx.Row,
) (price decimal.Decimal, coingeckoId string, ok bool, err error) {
	var p string
	if err = row.Scan(&coingeckoId, &p); err != nil {
		err = fmt.Errorf("unable to get pricepoint: %w", err)
		return
	}
	if p == "" {
		return
	}
	ok = true
	price, err = decimal.NewFromString(p)
	assert.NoErr(err, fmt.Sprintf("invalid price: %s", p))
	return
}

type EventData interface {
	SetPrice(decimal.Decimal)
}

type EventTransferInternal struct {
	FromAccount string          `json:"fromAccount"`
	ToAccount   string          `json:"toAccount"`
	Token       string          `json:"token"`
	Amount      decimal.Decimal `json:"amount"`
	Price       decimal.Decimal `json:"price"`
	Value       decimal.Decimal `json:"value"`
	Profit      decimal.Decimal `json:"profit"`
}

func (internal *EventTransferInternal) SetPrice(price decimal.Decimal) {
	internal.Price = price
	internal.Value = price.Mul(internal.Amount)
}

type EventTransferDirection uint8

const (
	EventTransferIncoming EventTransferDirection = iota
	EventTransferOutgoing
)

type EventTransfer struct {
	Direction EventTransferDirection `json:"direction"`
	Account   string                 `json:"account"`
	Token     string                 `json:"token"`
	Amount    decimal.Decimal        `json:"amount"`
	Price     decimal.Decimal        `json:"price"`
	Value     decimal.Decimal        `json:"value"`
	Profit    decimal.Decimal        `json:"profit"`
}

func (transfer *EventTransfer) SetPrice(price decimal.Decimal) {
	transfer.Price = price
	transfer.Value = price.Mul(transfer.Amount)
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
