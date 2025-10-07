package parser

import (
	"encoding/binary"
	"taxee/pkg/db"
)

const (
	SOL_SYSTEM_PROGRAM_ADDRESS = "11111111111111111111111111111111"
	SOL_MINT_ADDRESS           = "So11111111111111111111111111111111111111112"
)

type solSystemIx uint32

const (
	solSystemIxCreate           solSystemIx = 0
	solSystemIxTransfer         solSystemIx = 2
	solSystemIxCreateWithSeed   solSystemIx = 3
	solSystemIxTransferWithSeed solSystemIx = 11
)

func solSystemIxFromData(data []byte) (ix solSystemIx, method string, ok bool) {
	if len(data) < 4 {
		return
	}

	disc := binary.LittleEndian.Uint32(data)
	ok = true

	switch solSystemIx(disc) {
	case solSystemIxCreate:
		ix, method = solSystemIxCreate, "create_account"
	case solSystemIxTransfer:
		ix, method = solSystemIxTransfer, "transfer"
	case solSystemIxCreateWithSeed:
		ix, method = solSystemIxCreateWithSeed, "create_account"
	case solSystemIxTransferWithSeed:
		ix, method = solSystemIxTransferWithSeed, "transfer"
	default:
		ok = false
		return
	}

	return
}

func solParseSystemIxSolTransfer(
	ixType solSystemIx,
	accounts []string,
	data []byte,
) (from, to string, amount uint64, ok bool) {
	ok = true
	switch ixType {
	case solSystemIxCreate, solSystemIxTransfer:
		from, to = accounts[0], accounts[1]
		amount = binary.LittleEndian.Uint64(data[4:])
	case solSystemIxTransferWithSeed:
		from, to = accounts[0], accounts[2]
		amount = binary.LittleEndian.Uint64(data[4:])
	case solSystemIxCreateWithSeed:
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

func solPreprocessSystemIx(ctx *solanaContext, ix *db.SolanaInstruction) {
	ixType, _, ok := solSystemIxFromData(ix.Data)
	if !ok {
		return
	}

	_, to, amount, ok := solParseSystemIxSolTransfer(ixType, ix.Accounts, ix.Data)
	if !ok {
		return
	}

	ctx.receiveSol(to, amount)
}

func solProcessSystemIx(
	ctx *solanaContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	ixType, method, ok := solSystemIxFromData(ix.Data)
	if !ok {
		return
	}

	from, to, amount, ok := solParseSystemIxSolTransfer(ixType, ix.Accounts, ix.Data)
	if !ok {
		return
	}

	fromInternal := ctx.walletOwned(from)
	toInternal := ctx.walletOwned(to) ||
		ctx.findOwned(ctx.slot, ctx.ixIdx, to) != nil

	if !fromInternal && !toInternal {
		return
	}

	event := db.Event{
		UiAppName:    "system_program",
		UiMethodName: method,
		UiType:       db.UiEventTransfer,
		Transfers:    make([]*db.EventTransfer, 0),
	}
	ctx.initEvent(&event)

	if fromInternal {
		transfer := db.EventTransfer{
			Type:    db.EventTransferOutgoing | db.EventTransferInternal,
			Account: from,
			Token:   SOL_MINT_ADDRESS,
		}
		transfer.WithRawAmount(amount, 9)

		if toInternal {
			transfer.Type |= db.EventTransferInternal
		}

		event.Transfers = append(event.Transfers, &transfer)
	}

	if toInternal {
		transfer := db.EventTransfer{
			Type:    db.EventTransferIncoming | db.EventTransferInternal,
			Account: to,
			Token:   SOL_MINT_ADDRESS,
		}
		transfer.WithRawAmount(amount, 9)

		if fromInternal {
			transfer.Type |= db.EventTransferInternal
		}

		event.Transfers = append(event.Transfers, &transfer)
	}

	*events = append(*events, &event)
}
