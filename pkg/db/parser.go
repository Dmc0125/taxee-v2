package db

import (
	"encoding/json"
	"math/big"
	"taxee/pkg/assert"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

type GlobalEvent struct {
	Network       Network
	ProgramId     string
	Discriminator [8]byte
}

type EventType string

const (
	EventTransfer EventType = "transfer"
)

// type Event struct {
// 	UserAccountId int32,
//
// }

type Event interface {
	EventType() EventType
}

type TransferType byte

const (
	TransferTypeInternal TransferType = 1
	TransferTypeIncoming TransferType = 2
	TransferTypeOutgoing TransferType = 3

	TransferTypeTransfer TransferType = 1 << 7
	TransferTypeClose    TransferType = 1 << 6
)

type TransferEvent struct {
	Type   TransferType    `json:"type"`
	From   string          `json:"from"`
	To     string          `json:"to"`
	Amount decimal.Decimal `json:"amount"`
	Token  string          `json:"token"`
}

func (transfer *TransferEvent) WithRawAmount(raw uint64, decimals uint8) {
	transfer.Amount = decimal.NewFromBigInt(
		new(big.Int).SetUint64(raw),
		-int32(decimals),
	)
}

func (transfer *TransferEvent) EventType() EventType {
	return EventTransfer
}

func EnqueueInsertUserEvent(
	batch *pgx.Batch,
	userAccountId int32,
	txId string,
	ixIdx int32,
	idx int32,
	event Event,
) *pgx.QueuedQuery {
	marshalled, err := json.Marshal(event)
	assert.NoErr(err, "unable to marshal event")

	const q = `
		insert into user_event (
			user_account_id, tx_id, ix_idx, idx,
			type, data
		) values (
			$1, $2, $3, $4, $5, $6
		)
	`
	return batch.Queue(
		q, userAccountId, txId, ixIdx, idx, event.EventType(), marshalled,
	)
}
