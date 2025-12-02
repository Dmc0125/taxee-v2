package parser

import (
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

const SOL_MANGO_V4_PROGRAM_ADDRESS = "zF2vSz6V9g1YHGmfrzsY497NJzbRr84QUrPry4bLQ25"

var (
	mangoV4AccountCreate = [8]uint8{198, 95, 39, 197, 41, 214, 157, 18}
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
	}
}

func solProcessMangoV4Ix(
	ctx *solContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "mango_v4"

	if disc == mangoV4AccountCreate {
		event, ok, err := solProcessAnchorInitAccount(
			ctx,
			&solInnerIxIterator{innerIxs: ix.InnerInstructions},
		)
		assert.NoErr(err, "")
		if !ok {
			return
		}

		event.App = app
		event.Method = "account_create"

		*events = append(*events, event)
		return
	}

	depositWithdraw := func(disc [8]uint8) bool {
		switch disc {
		// token deposit
		case [8]uint8{117, 255, 154, 71, 245, 58, 95, 89}:
			solNewLendingDepositWithdrawEvent(
				ctx, events,
				ix.Accounts[2], ix.Accounts[1],
				ix.InnerInstructions[0],
				app,
			)
			return true
		// token withdraw
		case [8]uint8{63, 223, 42, 59, 15, 128, 102, 66}:
		}

		return false
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
			ctx, events,
			ix.Accounts[1], ix.InnerInstructions[0],
			direction, app, method,
		)
	}
}
