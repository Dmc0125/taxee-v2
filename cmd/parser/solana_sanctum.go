package parser

import "taxee/pkg/db"

const SOL_SANCTUM_PROGRAM_ADDRESS = "5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx"

func solProcessSanctumIx(ctx *solContext, ix *db.SolanaInstruction) {
	const app = "sanctum"
	disc := ix.Data[0]

	switch disc {
	case 3:
		solSwapEventFromTransfers(ctx, ix.InnerInstructions, app, "swap", db.EventTypeSwap)
	}
}
