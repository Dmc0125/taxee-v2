package db

import (
	"encoding/json"
	"fmt"
	"taxee/pkg/assert"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

func EnqueueInsertCoingeckoToken(
	batch *pgx.Batch,
	network Network,
	coingeckoId,
	token string,
) *pgx.QueuedQuery {
	const q = `
		insert into coingecko_token (
			network, coingecko_id, token
		) values (
			$1, $2, $3
		) on conflict (
			network, coingecko_id, token
		) do nothing
	`
	return batch.Queue(q, network, coingeckoId, token)
}

func EnqueueInsertPricepoint(
	batch *pgx.Batch,
	price decimal.Decimal,
	timestamp time.Time,
	coingeckoId string,
) *pgx.QueuedQuery {
	const q = `
		insert into pricepoint (
			price, timestamp, token_id
		) values (
			$1, $2, (
				select ct.id from coingecko_token ct where
					ct.coingecko_id = $3
			)
		) on conflict (
			timestamp, token_id
		) do nothing
	`
	return batch.Queue(q, price, timestamp, coingeckoId)
}

const qGetPricepoint = `
	select
		ct.coingecko_id,
		(
			case
				when pp.token_id is not null then pp.price
				else ''
			end
		) as price
	from
		coingecko_token ct
	left join
		pricepoint pp on
			pp.token_id = ct.id and pp.timestamp = $3
	where
		ct.network = $1 and ct.token = $2
`

// func GetPricepoint(
// 	ctx context.Context,
// 	pool *pgxpool.Pool,
// 	network Network,
// 	token string,
// 	timestamp time.Time,
// ) (price decimal.Decimal, coingeckoId string, ok bool, err error) {
// 	row := pool.QueryRow(ctx, qGetPricepoint, network, token, timestamp)
//
// }

func EnqueueGetPricepoint(
	batch *pgx.Batch,
	network Network,
	token string,
	timestamp time.Time,
) *pgx.QueuedQuery {
	return batch.Queue(qGetPricepoint, network, token, timestamp)
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
