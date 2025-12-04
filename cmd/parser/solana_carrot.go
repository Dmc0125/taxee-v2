package parser

import (
	"slices"
	"taxee/pkg/db"
)

const SOL_CARROT_PROGRAM_ADDRESS = "CarrotwivhMpDnm27EHmRLeQ683Z1PufuqEmBZvD282s"

func solProcessCarrotIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	switch disc {
	// issue
	case [8]uint8{190, 1, 98, 214, 81, 99, 222, 247}:
		owner := ix.Accounts[6]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}
		solSwapEventFromTransfers(
			ctx, ix.InnerInstructions,
			"carrot", "swap",
			db.EventTypeSwap,
		)
	}
}
