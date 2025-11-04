package parser

import (
	"taxee/pkg/db"
)

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
	ok = true

	switch disc {
	// routeV2
	case [8]uint8{187, 100, 250, 204, 49, 196, 175, 20}:
		ix, method = solJupIxRouteV2, "route_v2"
	default:
		ok = false
		return
	}

	return
}

func solProcessJupV6(
	ctx *solanaContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	ixType, _, ok := solJupV6IxFromData(ix.Data)
	if !ok {
		return
	}

	switch ixType {
	case solJupIxRouteV2:
		owner := ix.Accounts[0]
		if !ctx.walletOwned(owner) {
			return
		}

		transfers := make([]*db.EventTransfer, 0)

		for _, innerIx := range ix.InnerInstructions {
			if innerIx.ProgramAddress != SOL_TOKEN_PROGRAM_ADDRESS {
				continue
			}

			ixType, _, ok := solTokenIxFromByte(innerIx.Data[0])
			if !ok {
				continue
			}

			from, to, amount, ok := solParseTokenIxTokenTransfer(
				ixType, innerIx.Accounts, innerIx.Data,
			)
			if !ok || amount == 0 {
				continue
			}

			var direction db.EventTransferDirection
			var tokenAccountData *SolTokenAccountData
			var tokenAccount string

			switch {
			case to != "":
				if toAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, to); toAccount != nil {
					direction = db.EventTransferIncoming
					tokenAccountData = solAccountDataMust[SolTokenAccountData](toAccount)
					tokenAccount = to
				}
			case from != "":
				if fromAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, from); fromAccount != nil {
					tokenAccountData = solAccountDataMust[SolTokenAccountData](fromAccount)
					direction = db.EventTransferOutgoing
					tokenAccount = from
				}
			}

			if tokenAccountData != nil {
				decimals := solDecimalsMust(ctx, tokenAccountData.Mint)
				transfer := db.EventTransfer{
					Direction:   direction,
					Account:     tokenAccount,
					Token:       tokenAccountData.Mint,
					Amount:      newDecimalFromRawAmount(amount, decimals),
					TokenSource: uint16(db.NetworkSolana),
				}
				transfers = append(transfers, &transfer)
			}
		}
	}
}
