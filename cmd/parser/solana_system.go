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

type solSystemTransfer struct {
	ix     solSystemIx
	method string
	from   string
	to     string
	amount uint64
}

func (t *solSystemTransfer) intoEventTransfer(
	fromWallet, toWallet string,
	direction db.EventTransferDirection,
) *db.EventTransfer {
	return &db.EventTransfer{
		Direction:   direction,
		FromWallet:  fromWallet,
		FromAccount: t.from,
		ToWallet:    toWallet,
		ToAccount:   t.to,
		Token:       SOL_MINT_ADDRESS,
		Amount:      newDecimalFromRawAmount(t.amount, 9),
		TokenSource: uint16(db.NetworkSolana),
	}
}

func solParseSystemIxSolTransfer(
	accounts []string,
	data []byte,
) (t *solSystemTransfer, ok bool) {
	if len(data) < 4 {
		return
	}

	t = new(solSystemTransfer)

	t.ix = solSystemIx(binary.LittleEndian.Uint32(data))
	ok = true

	switch t.ix {
	case solSystemIxCreate:
		t.method = "create_account"
		t.from, t.to = accounts[0], accounts[1]
		t.amount = binary.LittleEndian.Uint64(data[4:])
	case solSystemIxTransfer:
		t.method = "transfer"
		t.from, t.to = accounts[0], accounts[1]
		t.amount = binary.LittleEndian.Uint64(data[4:])
	case solSystemIxTransferWithSeed:
		t.method = "transfer"
		t.from, t.to = accounts[0], accounts[2]
		t.amount = binary.LittleEndian.Uint64(data[4:])
	case solSystemIxCreateWithSeed:
		t.method = "create_account"
		t.from, t.to = accounts[0], accounts[1]

		// amount offset -> 4 (disc) + 32 (base address) + 4 (seed len) +  seed len * u8
		seedLen := binary.LittleEndian.Uint64(data[36:])
		offset := 44 + seedLen
		t.amount = binary.LittleEndian.Uint64(data[offset:])
	default:
		ok = false
	}
	return
}

func solPreprocessSystemIx(ctx *solContext, ix *db.SolanaInstruction) {
	transfer, ok := solParseSystemIxSolTransfer(ix.Accounts, ix.Data)
	if !ok {
		return
	}

	ctx.receiveSol(transfer.to, transfer.amount)
}

func solProcessSystemIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
) {
	t, ok := solParseSystemIxSolTransfer(ix.Accounts, ix.Data)
	if !ok || t.amount == 0 {
		return
	}

	fromInternal := ctx.walletOwned(t.from)
	toInternalWallet := ctx.walletOwned(t.to)
	toAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, t.to)

	toWallet, toInternal := t.to, toInternalWallet
	if toAccount != nil {
		toInternal = true
		switch data := toAccount.Data.(type) {
		case *solTokenAccountData:
			toWallet = data.Owner
		}
	}

	if !fromInternal && !toInternal {
		return
	}

	event := solNewEvent(ctx, "system", t.method, db.EventTypeTransfer)
	event.Transfers = append(event.Transfers, t.intoEventTransfer(
		t.from, toWallet,
		getTransferEventDirection(fromInternal, toInternal),
	))
}
