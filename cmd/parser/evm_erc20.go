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

	evmErc20Id uint32 = evmErc20TotalSupply ^
		evmErc20BalanceOf ^
		evmErc20Transfer ^
		evmErc20Allowance ^
		evmErc20Approve ^
		evmErc20TransferFrom
)

func evmIdentifyErc20Contract(bytecode []uint8) bool {
	var selectors [6]uint8

	for i := 0; i < len(bytecode); i += 1 {
		b := bytecode[i]

		// NOTE: PUSH4 ix
		if b == 0x63 {
			selector := binary.BigEndian.Uint32(bytecode[i+1 : i+5])
			eqIx := bytecode[i+5]
			i += 6
			if i >= len(bytecode) {
				return false
			}
			// NOTE: the instruction following the selector should be EQ (0x14)
			if eqIx != 20 {
				continue
			}

			switch selector {
			case evmErc20TotalSupply:
				// totalSupply()
				selectors[0] += 1
			case evmErc20BalanceOf:
				// balanceOf(address)
				selectors[1] += 1
			case evmErc20Transfer:
				// transfer(address,uint256)
				selectors[2] += 1
			case evmErc20Allowance:
				// allowance(address,address)
				selectors[3] += 1
			case evmErc20Approve:
				// approve(address,uint256)
				selectors[4] += 1
			case evmErc20TransferFrom:
				// transferFrom(address,address,uint256)
				selectors[5] += 1
			}
		}
	}

	for _, m := range selectors {
		if m != 1 {
			return false
		}
	}

	return true
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
	// TODO: endianness?
	amount := new(big.Int).SetBytes(data)
	return decimal.NewFromBigInt(amount, 0)
}

func evmProcessErc20Tx(
	ctx *evmContext,
	events *[]*db.Event,
	tx *db.EvmTransactionData,
) {
	method := binary.BigEndian.Uint32(tx.Input[:4])
	data := tx.Input[4:]

	var from, to string
	var amount decimal.Decimal

	switch method {
	case evmErc20Transfer:
		from = tx.From
		to = evmAddressFrom32Bytes(data[:32])
		amount = evmAmountFrom32Bytes(data[32:])
	case evmErc20TransferFrom:
		from = evmAddressFrom32Bytes(data[:32])
		to = evmAddressFrom32Bytes(data[32:64])
		amount = evmAmountFrom32Bytes(data[64:])
	default:
		return
	}

	fromInternal, toInternal := evmWalletOwned(ctx, from), evmWalletOwned(ctx, to)

	event := newEvmEvent(ctx)
	event.UiAppName = "erc20"
	event.UiMethodName = "transfer"

	switch {
	case fromInternal && toInternal:
		event.Type = db.EventTypeTransferInternal
		event.Data = &db.EventTransferInternal{
			FromAccount: from,
			ToAccount:   to,
			Token:       tx.To,
			Amount:      amount,
		}
	case fromInternal:
		event.Type = db.EventTypeTransfer
		event.Data = &db.EventTransfer{
			Direction: db.EventTransferOutgoing,
			Account:   from,
			Token:     tx.To,
			Amount:    amount,
		}
	case toInternal:
		event.Type = db.EventTypeTransfer
		event.Data = &db.EventTransfer{
			Direction: db.EventTransferIncoming,
			Account:   to,
			Token:     tx.To,
			Amount:    amount,
		}
	default:
		return
	}

	*events = append(*events, event)
}
