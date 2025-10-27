package parser

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"taxee/pkg/assert"
	"taxee/pkg/db"

	"github.com/shopspring/decimal"
)

const (
	evmErc20TotalSupply  uint32 = 0x18160ddd
	evmErc20BalanceOf    uint32 = 0x70a08231
	evmErc20Transfer     uint32 = 0xa9059cbb
	evmErc20Allowance    uint32 = 0xdd62ed3e
	evmErc20Approve      uint32 = 0x095ea7b3
	evmErc20TransferFrom uint32 = 0x23b872dd

	evmErc20ContractId uint32 = evmErc20TotalSupply ^
		evmErc20BalanceOf ^
		evmErc20Transfer ^
		evmErc20Allowance ^
		evmErc20Approve ^
		evmErc20TransferFrom
)

var evmErc20Selectors = map[uint32]bool{
	evmErc20TotalSupply:  true,
	evmErc20BalanceOf:    true,
	evmErc20Transfer:     true,
	evmErc20Allowance:    true,
	evmErc20Approve:      true,
	evmErc20TransferFrom: true,
}

func evmAddressFrom32Bytes(data []byte) string {
	assert.True(len(data) == 32, "invalid data len")
	addressBytes := data[12:]
	return fmt.Sprintf(
		"0x%s",
		hex.EncodeToString(addressBytes),
	)
}

func evmAmountFrom32Bytes(data []byte) decimal.Decimal {
	assert.True(len(data) == 32, "invalid data len")
	amount := new(big.Int).SetBytes(data)
	return decimal.NewFromBigInt(amount, 0)
}

func evmProcessErc20Tx(
	ctx *evmContext,
	events *[]*db.Event,
	tx *db.EvmTransactionData,
) {
	selector := binary.BigEndian.Uint32(tx.Input[:4])
	data := tx.Input[4:]

	var from, to, method string
	var amount decimal.Decimal

	switch selector {
	case evmErc20Transfer:
		method = "transfer"
		from = tx.From
		to = evmAddressFrom32Bytes(data[:32])
		amount = evmAmountFrom32Bytes(data[32:])
	case evmErc20TransferFrom:
		method = "transfer_from"
		from = evmAddressFrom32Bytes(data[:32])
		to = evmAddressFrom32Bytes(data[32:64])
		amount = evmAmountFrom32Bytes(data[64:])
	default:
		return
	}

	fromInternal, toInternal := evmWalletOwned(ctx, from), evmWalletOwned(ctx, to)

	if !fromInternal && !toInternal {
		return
	}

	event := evmNewEvent(ctx)
	event.UiAppName = "erc20"
	event.UiMethodName = method

	setEventTransfer(
		event,
		from, to,
		fromInternal, toInternal,
		amount,
		tx.To,
		uint16(ctx.network),
	)

	*events = append(*events, event)
}
