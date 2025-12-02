package parser

import (
	"taxee/pkg/db"
)

const SOL_JUP_V6_PROGRAM_ADDRESS = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

type solJupV6Ix uint16

const (
	solJupIxRouteV2 solJupV6Ix = iota
)

func solProcessJupV6(
	ctx *solContext,
	ix *db.SolanaInstruction,
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	var owner string

	switch disc {
	// sharedAccountsRoute
	case [8]uint8{193, 32, 155, 51, 65, 214, 156, 129}:
		owner = ix.Accounts[2]
	// routeV2
	case [8]uint8{187, 100, 250, 204, 49, 196, 175, 20}:
		owner = ix.Accounts[0]
	// route
	case [8]uint8{229, 23, 203, 151, 122, 227, 173, 42}:
		owner = ix.Accounts[1]
	default:
		return
	}

	if !ctx.walletOwned(owner) {
		return
	}

	solSwapEventFromTransfers(
		ctx,
		ix.InnerInstructions,
		"jupiter",
		"swap",
		db.EventTypeSwap,
	)
}
