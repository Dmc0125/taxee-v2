package parser

import (
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

const SOL_CIRCUIT_PROGRAM_ADDRESS = "vAuLTsyrvSfZRuRB3XgvkPwNGgYSs9YRYymVebLKoxR"

var (
	circuitCreateAccountIx = [8]uint8{112, 174, 162, 232, 89, 92, 205, 168}
)

func solPreprocessCircuitIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	switch disc {
	case circuitCreateAccountIx:
		owner := ix.Accounts[2]
		err := solPreprocessAnchorInitAccount(ctx, owner, ix.InnerInstructions)
		assert.NoErr(err, "")
	}
}

func solProcessCircuitIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "circuit_trading"

	if disc == circuitCreateAccountIx {
		_, err := solProcessAnchorInitAccount(
			ctx,
			&solInnerIxIterator{innerIxs: ix.InnerInstructions},
			ix.Accounts[2], app, "create_account",
		)
		assert.NoErr(err, "")
		return
	}

	var transferIx *db.SolanaInnerInstruction
	var method string
	var direction int

	switch disc {
	// deposit
	case [8]uint8{242, 35, 198, 137, 82, 225, 242, 182}:
		transferIx = ix.InnerInstructions[0]
		method = "deposit"
		direction = 0
	// withdraw
	case [8]uint8{183, 18, 70, 156, 148, 109, 161, 34}:
		transferIx = ix.InnerInstructions[2]
		method = "withdraw"
		direction = 1
	default:
		return
	}

	solNewLendingStakeEvent(
		ctx,
		ix.Accounts[2], ix.Accounts[1], transferIx,
		direction, app, method,
	)
}
