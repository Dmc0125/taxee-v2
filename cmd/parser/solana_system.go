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
		seedLen := binary.LittleEndian.Uint64(data[36:])
		offset := 44 + seedLen
		amount = binary.LittleEndian.Uint64(data[offset:])
	default:
		// TODO: swap to assert
		ok = false
	}
	return
}

func solPreprocessSystemIx(ctx *solContext, ix *db.SolanaInstruction) {
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
	ctx *solContext,
	ix *db.SolanaInstruction,
) {
	ixType, method, ok := solSystemIxFromData(ix.Data)
	if !ok {
		return
	}

	from, to, amount, ok := solParseSystemIxSolTransfer(ixType, ix.Accounts, ix.Data)
	if !ok || amount == 0 {
		return
	}

	fromInternal := ctx.walletOwned(from)
	toInternalWallet := ctx.walletOwned(to)
	toAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, to)

	toWallet, toInternal := to, toInternalWallet
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

	event := solNewEvent(ctx, "system", method, db.EventTypeTransfer)
	event.Transfers = append(event.Transfers, &db.EventTransfer{
		Direction:   getTransferEventDirection(fromInternal, toInternal),
		FromWallet:  from,
		ToWallet:    toWallet,
		FromAccount: from,
		ToAccount:   to,
		Token:       SOL_MINT_ADDRESS,
		Amount:      newDecimalFromRawAmount(amount, 9),
		TokenSource: uint16(db.NetworkSolana),
	})
}
