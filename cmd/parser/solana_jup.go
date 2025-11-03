package parser

import "taxee/pkg/db"

const SOL_JUP_V6_PROGRAM_ADDRESS = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

type solJupV6Ix uint16

const (
	solJupIxRouteV2 solJupV6Ix = iota
)

func solJupV6IxFromData(data []byte) (ix solJupV6Ix, method string, ok bool) {
	if len(data) < 8 {
		return
	}

	var disc [8]byte
	copy(disc[:], data[:8])

	switch disc {

	}
}

func solPreprocessJupV6Ix(ctx *solanaContext, ix *db.SolanaInstruction) {
}
