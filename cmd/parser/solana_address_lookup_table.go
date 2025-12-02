package parser

import (
	"encoding/binary"
	"slices"
	"taxee/pkg/db"
)

const SOL_ALT_PROGRAM_ADDRESS = "AddressLookupTab1e1111111111111111111111111"

func solPreprocessAltIx(ctx *solContext, ix *db.SolanaInstruction) {
	if len(ix.Data) < 4 {
		return
	}
	disc := binary.LittleEndian.Uint32(ix.Data)

	switch disc {
	// Create, extend
	case 0, 2:
		owner := ix.Accounts[1]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		transferIx := ix.InnerInstructions[0]
		ixType, _, ok := solSystemIxFromData(transferIx.Data)
		if ok {
			_, to, amount, _ := solParseSystemIxSolTransfer(ixType, transferIx.Accounts, transferIx.Data)
			ctx.receiveSol(to, amount)
		}

		if disc == 0 {
			altAddress := ix.Accounts[0]
			ctx.init(altAddress, true, nil)
		}
	}
}

func solProcessAltIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
) {
	if len(ix.Data) < 4 {
		return
	}
	disc := binary.LittleEndian.Uint32(ix.Data)

	const app = "address_lookup_table"
	var method string

	switch disc {
	// create
	case 0:
		method = "create"
	// extend
	case 2:
		method = "extend"
	default:
		return
	}

	if len(ix.InnerInstructions) == 0 {
		return
	}

	transferIx := ix.InnerInstructions[0]
	ixType, _, ok := solSystemIxFromData(transferIx.Data)
	if !ok {
		return
	}
	from, to, amount, _ := solParseSystemIxSolTransfer(ixType, transferIx.Accounts, transferIx.Data)

	fromInternal := slices.Contains(ctx.wallets, from)
	toInternal := ctx.findOwned(ctx.slot, ctx.ixIdx, to) != nil

	if !fromInternal && !toInternal {
		return
	}

	var toWallet string
	direction := getTransferEventDirection(fromInternal, toInternal)
	if direction != db.EventTransferOutgoing {
		// owner
		toWallet = ix.Accounts[1]
	}

	event := solNewEvent(ctx, app, method, db.EventTypeTransfer)
	event.Transfers = append(event.Transfers, &db.EventTransfer{
		Direction:   direction,
		FromWallet:  from,
		FromAccount: from,
		ToWallet:    toWallet,
		ToAccount:   to,
		Token:       SOL_MINT_ADDRESS,
		Amount:      newDecimalFromRawAmount(amount, 9),
		TokenSource: uint16(db.NetworkSolana),
	})
}
