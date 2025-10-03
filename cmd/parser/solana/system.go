package solana

import (
	"encoding/binary"
	"taxee/pkg/db"
)

const (
	SYSTEM_PROGRAM_ADDRESS = "11111111111111111111111111111111"
	SOL_MINT_ADDRESS       = "So11111111111111111111111111111111111111112"
)

type systemIx uint32

const (
	systemIxCreate           systemIx = 0
	systemIxTransfer         systemIx = 2
	systemIxCreateWithSeed   systemIx = 3
	systemIxTransferWithSeed systemIx = 11
)

func systemIxFromData(data []byte) (ix systemIx, ok bool) {
	if len(data) < 4 {
		return
	}

	disc := binary.LittleEndian.Uint32(data)
	ok = true

	switch systemIx(disc) {
	case systemIxCreate:
		ix = systemIxCreate
	case systemIxTransfer:
		ix = systemIxTransfer
	case systemIxCreateWithSeed:
		ix = systemIxCreateWithSeed
	case systemIxTransferWithSeed:
		ix = systemIxTransferWithSeed
	default:
		ok = false
		return
	}

	return
}

func parseSystemIxSolTransfer(
	ixType systemIx,
	accounts []string,
	data []byte,
) (from, to string, amount uint64, ok bool) {
	ok = true
	switch ixType {
	case systemIxCreate, systemIxTransfer:
		from, to = accounts[0], accounts[1]
		amount = binary.LittleEndian.Uint64(data[4:])
	case systemIxTransferWithSeed:
		from, to = accounts[0], accounts[2]
		amount = binary.LittleEndian.Uint64(data[4:])
	case systemIxCreateWithSeed:
		from, to = accounts[0], accounts[1]

		// amount offset -> 4 (disc) + 32 (base address) + 4 (seed len) +  seed len * u8
		seedLen := binary.LittleEndian.Uint32(data[36:])
		offset := 40 + seedLen
		amount = binary.LittleEndian.Uint64(data[offset:])
	default:
		ok = false
	}
	return
}

func preprocessSystemIx(ctx *Context, ix *db.SolanaInstruction) {
	ixType, ok := systemIxFromData(ix.Data)
	if !ok {
		return
	}

	_, to, amount, ok := parseSystemIxSolTransfer(ixType, ix.Accounts, ix.Data)
	if !ok {
		return
	}

	ctx.accountReceiveSol(to, amount)
}

func processSystemIx(
	ctx *Context,
	events *[]db.Event,
	ix *db.SolanaInstruction,
) {
	ixType, ok := systemIxFromData(ix.Data)
	if !ok {
		return
	}

	from, to, amount, ok := parseSystemIxSolTransfer(ixType, ix.Accounts, ix.Data)
	if !ok {
		return
	}

	fromInternal := ctx.WalletOwned(db.NetworkSolana, from)
	toInternal := ctx.WalletOwned(db.NetworkSolana, to) || ctx.accountFindOwned(to, ctx.Slot, ctx.IxIdx) != nil

	event := &db.TransferEvent{
		Type:  db.TransferTypeTransfer,
		From:  from,
		To:    to,
		Token: SOL_MINT_ADDRESS,
	}
	event.WithRawAmount(amount, 9)

	switch {
	case fromInternal && toInternal:
		event.Type |= db.TransferTypeInternal
	case fromInternal && !toInternal:
		event.Type |= db.TransferTypeOutgoing
	case !fromInternal && toInternal:
		event.Type |= db.TransferTypeIncoming
	default:
		return
	}

	*events = append(*events, event)
}
