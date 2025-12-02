package parser

import (
	"slices"
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

const SOL_MANGO_V4_PROGRAM_ADDRESS = "zF2vSz6V9g1YHGmfrzsY497NJzbRr84QUrPry4bLQ25"
const SOL_MANGO_V4_PROGRAM_ADDRESS_2 = "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg"

var (
	mangoV4AccountCreate = [8]uint8{198, 95, 39, 197, 41, 214, 157, 18}
	mangoV4AccountClose  = [8]uint8{115, 5, 192, 28, 86, 221, 137, 102}
)

func solPreprocessMangoV4Ix(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	switch disc {
	case mangoV4AccountCreate:
		owner := ix.Accounts[2]
		err := solPreprocessAnchorInitAccount(ctx, owner, ix.InnerInstructions)
		assert.NoErr(err, "")
	case mangoV4AccountClose:
		owner := ix.Accounts[2]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}
		ctx.close(ix.Accounts[1])
	}
}

func solProcessMangoV4Ix(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "mango_v4"

	if disc == mangoV4AccountCreate {
		_, err := solProcessAnchorInitAccount(
			ctx,
			&solInnerIxIterator{innerIxs: ix.InnerInstructions},
			ix.Accounts[2], app, "account_create",
		)
		assert.NoErr(err, "")
		return
	}

	if disc == mangoV4AccountClose {
		owner, receiver := ix.Accounts[2], ix.Accounts[3]
		ownerInternal := slices.Contains(ctx.wallets, owner)
		receiverInternal := slices.Contains(ctx.wallets, receiver)
		if !ownerInternal && !receiverInternal {
			return
		}

		account := ix.Accounts[1]

		event := solNewEvent(ctx, app, "account_close", db.EventTypeCloseAccount)
		event.Transfers = append(event.Transfers, &db.EventTransfer{
			Direction:   getTransferEventDirection(ownerInternal, receiverInternal),
			FromWallet:  owner,
			FromAccount: account,
			ToWallet:    receiver,
			ToAccount:   receiver,
			Token:       SOL_MINT_ADDRESS,
			TokenSource: uint16(db.NetworkSolana),
		})
	}

	depositWithdraw := func(disc [8]uint8) bool {
		var direction int
		var method string

		switch disc {
		// token deposit
		case [8]uint8{117, 255, 154, 71, 245, 58, 95, 89}:
			direction = 0
			method = "lend"
		// token withdraw
		case [8]uint8{63, 223, 42, 59, 15, 128, 102, 66}:
			direction = 1
			method = "withdraw"
		default:
			return false
		}

		solNewLendingStakeEvent(
			ctx,
			ix.Accounts[2], ix.Accounts[1],
			ix.InnerInstructions[0],
			direction, app, method,
		)
		return true
	}

	if depositWithdraw(disc) {
		return
	}

	var method string
	var direction db.EventTransferDirection

	switch disc {
	// flash loan
	case [8]uint8{81, 78, 224, 60, 244, 56, 90, 239}:
		method = "flash_loan"
		direction = db.EventTransferIncoming
	// flash repay
	case [8]uint8{38, 171, 16, 3, 140, 247, 119, 197}:
		method = "flash repay"
		direction = db.EventTransferOutgoing
	}

	if len(method) != 0 {
		solNewLendingBorrowRepayEvent(
			ctx,
			ix.Accounts[1], ix.InnerInstructions[0],
			direction, app, method,
		)
	}
}
