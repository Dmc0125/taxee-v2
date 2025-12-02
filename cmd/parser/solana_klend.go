package parser

import (
	"slices"
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

const SOL_KAMINO_LEND_PROGRAM_ADDRESS = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"

var (
	klendInitUserMetadataIx  = [8]uint8{117, 169, 176, 69, 197, 23, 15, 162}
	klendInitObligationIx    = [8]uint8{251, 10, 231, 76, 27, 11, 159, 96}
	klendInitFarmsForReserve = [8]uint8{136, 63, 15, 186, 211, 152, 168, 164}
)

func solPreprocessKaminoLendIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	var owner string
	var innerIxs []*db.SolanaInnerInstruction

	switch disc {
	case klendInitUserMetadataIx, klendInitObligationIx:
		owner = ix.Accounts[0]
		innerIxs = ix.InnerInstructions
	case klendInitFarmsForReserve:
		owner = ix.Accounts[1]
		innerIxs = ix.InnerInstructions[1:]
	default:
		return
	}

	err := solPreprocessAnchorInitAccount(ctx, owner, innerIxs)
	assert.NoErr(err, "")
}

func solProcessKaminoLendIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "kamino_lend"
	var method string

	{
		var owner string
		var innerIxs []*db.SolanaInnerInstruction

		switch disc {
		case klendInitUserMetadataIx:
			owner, method = ix.Accounts[0], "init_metadata"
			innerIxs = ix.InnerInstructions
		case klendInitObligationIx:
			owner, method = ix.Accounts[0], "init_obligation"
			innerIxs = ix.InnerInstructions
		case klendInitFarmsForReserve:
			owner, method = ix.Accounts[1], "init_farms"
			innerIxs = ix.InnerInstructions[1:]
		}

		if method != "" {
			if !slices.Contains(ctx.wallets, owner) {
				return
			}

			innerIxsIter := solInnerIxIterator{innerIxs: innerIxs}
			event, _, err := solProcessAnchorInitAccount(ctx, &innerIxsIter)
			assert.NoErr(err, "")

			event.App = app
			event.Method = method
			*events = append(*events, event)
			return
		}
	}

	{
		var direction db.EventTransferDirection

		switch disc {
		// flash borrow
		case [8]uint8{135, 231, 52, 167, 7, 52, 212, 193}:
			direction = db.EventTransferIncoming
			method = "flash_borrow"
		// flash repay
		case [8]uint8{185, 117, 0, 203, 96, 245, 180, 186}:
			direction = db.EventTransferOutgoing
			method = "flash_repay"
		// borrow
		case [8]uint8{121, 127, 18, 204, 73, 245, 225, 65}:
			direction = db.EventTransferIncoming
			method = "borrow"
		}

		if method != "" {
			solNewLendingBorrowRepayEvent(
				ctx, events,
				ix.Accounts[0], ix.InnerInstructions[0],
				direction, app, method,
			)
			return
		}
	}

	switch disc {
	// deposit
	case [8]uint8{129, 199, 4, 2, 222, 39, 26, 46}:
		solNewLendingDepositWithdrawEvent(
			ctx, events,
			ix.Accounts[0], ix.Accounts[1],
			ix.InnerInstructions[0],
			app,
		)
	}
}
