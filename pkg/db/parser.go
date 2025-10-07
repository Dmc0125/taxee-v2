package db

import (
	"encoding/json"
	"fmt"
	"math/big"
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

type EventTransferType byte

func (t EventTransferType) String() string {
	buf := [8]byte{}
	for i := 0; i < len(buf); i += 1 {
		c := (t >> (7 - i)) & 1
		switch c {
		case 1:
			buf[i] = '1'
		default:
			buf[i] = '0'
		}
	}
	return string(buf[:])
}

const (
	// types
	EventTransferTypeMask EventTransferType = 0b00001111
	EventTransferIncoming EventTransferType = iota
	EventTransferOutgoing

	// type flags
	EventTransferInternal EventTransferType = 1 << 7
	EventTransferMint
	EventTransferBurn
)

type EventTransfer struct {
	Type    EventTransferType
	Account string
	Token   string
	Amount  decimal.Decimal
	Price   decimal.Decimal
	Value   decimal.Decimal
	Profit  decimal.Decimal
}

func (transfer *EventTransfer) WithRawAmount(raw uint64, decimals uint8) {
	transfer.Amount = decimal.NewFromBigInt(
		new(big.Int).SetUint64(raw),
		int32(decimals),
	)
}

// TODO: assertions to check the correctness of the events
func NewEventTransfer(
	transferType EventTransferType,
	account, token string,
	amountRaw uint64,
	decimals uint8,
) EventTransfer {
	event := EventTransfer{
		Type:    transferType,
		Account: account,
		Token:   token,
	}
	event.WithRawAmount(amountRaw, decimals)
	return event
}

type UiEventType string

const (
	UiEventTransfer UiEventType = "transfer"
	UiEventSwap     UiEventType = "swap"
	// UiEventTransfer UiEventType = "transfer"
	// UiEventTransfer UiEventType = "transfer"
)

// TODO: assertions to check the correctness of the events
// like if the event is internal transfer, there will be 2 Transfers
// both of the **must** be internal
type Event struct {
	UiAppName    string
	UiMethodName string
	UiType       UiEventType
	Timestamp    time.Time
	Network      Network
	TxId         string
	IxIdx        int32
	Transfers    []*EventTransfer
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
			ui_app_name, ui_method_name, ui_type,
			transfers
		) values (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
	`

	data, err := json.Marshal(event.Transfers)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal transfers: %w", err)
	}

	return batch.Queue(
		q,
		userAccountId, event.TxId, event.Network, event.IxIdx, idx, event.Timestamp,
		event.UiAppName, event.UiMethodName, event.UiType, data,
	), nil
}
