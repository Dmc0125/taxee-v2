package parser

import (
	"encoding/binary"
	"math"
	"slices"
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

const SOL_KAMINO_LEND_PROGRAM_ADDRESS = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"

var (
	klendInitUserMetadataIx  = [8]uint8{117, 169, 176, 69, 197, 23, 15, 162}
	klendInitObligationIx    = [8]uint8{251, 10, 231, 76, 27, 11, 159, 96}
	klendInitFarmsForReserve = [8]uint8{136, 63, 15, 186, 211, 152, 168, 164}
	klendWithdrawIx          = [8]uint8{75, 93, 93, 220, 34, 150, 218, 196}
)

func solPreprocessKaminoLendIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	if disc == klendWithdrawIx {
		owner, obligation := ix.Accounts[0], ix.Accounts[1]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		amount := binary.LittleEndian.Uint64(ix.Data[8:])
		if amount == math.MaxUint64 {
			ctx.close(obligation)
		}
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
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "kamino_lend"

	initIx := func() (owner, method string, innerIxs []*db.SolanaInnerInstruction, ok bool) {
		switch disc {
		case klendInitUserMetadataIx:
			owner, method = ix.Accounts[0], "init_metadata"
			innerIxs = ix.InnerInstructions
			ok = true
		case klendInitObligationIx:
			owner, method = ix.Accounts[0], "init_obligation"
			innerIxs = ix.InnerInstructions
			ok = true
		case klendInitFarmsForReserve:
			owner, method = ix.Accounts[1], "init_farms"
			innerIxs = ix.InnerInstructions[1:]
			ok = true
		}
		return
	}

	if owner, method, innerIxs, ok := initIx(); ok {
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		innerIxsIter := solInnerIxIterator{innerIxs: innerIxs}
		_, err := solProcessAnchorInitAccount(
			ctx, &innerIxsIter, owner, app, method,
		)
		assert.NoErr(err, "")
		return
	}

	borrowRepayIx := func() (method string, dir db.EventTransferDirection, transferIx *db.SolanaInnerInstruction, ok bool) {
		switch disc {
		// flash borrow
		case [8]uint8{135, 231, 52, 167, 7, 52, 212, 193}:
			dir = db.EventTransferIncoming
			method = "flash_borrow"
			transferIx = ix.InnerInstructions[0]
			ok = true
		// flash repay
		case [8]uint8{185, 117, 0, 203, 96, 245, 180, 186}:
			dir = db.EventTransferOutgoing
			method = "flash_repay"
			transferIx = ix.InnerInstructions[0]
			ok = true
		// borrow
		case [8]uint8{121, 127, 18, 204, 73, 245, 225, 65}:
			dir = db.EventTransferIncoming
			method = "borrow"
			transferIx = ix.InnerInstructions[len(ix.InnerInstructions)-1]
			ok = true
		// repay
		case [8]uint8{145, 178, 13, 225, 76, 240, 147, 72}:
			dir = db.EventTransferOutgoing
			method = "repay"
			transferIx = ix.InnerInstructions[0]
			ok = true
		}
		return
	}

	if method, dir, transferIx, ok := borrowRepayIx(); ok {
		solNewLendingBorrowRepayEvent(
			ctx,
			ix.Accounts[0], transferIx,
			dir, app, method,
		)
		return
	}

	switch disc {
	// deposit
	case [8]uint8{129, 199, 4, 2, 222, 39, 26, 46}:
		solNewLendingStakeEvent(
			ctx,
			ix.Accounts[0], ix.Accounts[1],
			ix.InnerInstructions[0],
			0, app, "lend",
		)
	// withdraw
	case klendWithdrawIx:
		owner, obligation := ix.Accounts[0], ix.Accounts[1]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		solNewLendingStakeEvent(
			ctx,
			owner, obligation,
			ix.InnerInstructions[len(ix.InnerInstructions)-1],
			1, app, "withdraw",
		)

		// TODO: this is not correct and needs to be done by tracking
		// deposits/withdrawals
		amount := binary.LittleEndian.Uint64(ix.Data[8:])
		if amount == math.MaxUint64 {
			event := solNewEvent(ctx, app, "close_account", db.EventTypeCloseAccount)
			event.Transfers = append(event.Transfers, &db.EventTransfer{
				Direction:   db.EventTransferInternal,
				FromWallet:  owner,
				FromAccount: obligation,
				ToWallet:    owner,
				ToAccount:   owner,
				Token:       SOL_MINT_ADDRESS,
				TokenSource: uint16(db.NetworkSolana),
			})
		}

	default:
		return
	}

}
