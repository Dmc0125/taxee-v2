package parser

import (
	"slices"
	"taxee/pkg/db"
)

const SOL_DFLOW_PROGRAM_ADDRESS = "DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH"

func solProcessDflowIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "dflow"

	switch disc {
	// fee
	case [8]uint8{129, 164, 196, 21, 177, 48, 180, 162}:
		owner := ix.Accounts[2]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		innerIxs := solInnerIxIterator{innerIxs: ix.InnerInstructions}
		if transferIx, ok := innerIxs.nextSafe(); ok {
			ixType, _, ok := solTokenIxFromByte(transferIx.Data[0])
			if !ok || ixType != solTokenIxTransfer {
				return
			}
			amount, from, _ := solParseTokenTransfer(
				transferIx.Accounts,
				transferIx.Data,
			)

			_, fromAccountData, ok := solAccountExactOrError[solTokenAccountData](
				ctx, from,
			)
			if !ok {
				return
			}

			decimals := solDecimalsMust(ctx, fromAccountData.Mint)

			event := solNewEvent(ctx, app, "fee", db.EventTypeTransfer)
			event.Transfers = append(event.Transfers, &db.EventTransfer{
				Direction:   db.EventTransferOutgoing,
				FromWallet:  owner,
				FromAccount: from,
				Token:       fromAccountData.Mint,
				Amount:      newDecimalFromRawAmount(amount, decimals),
				TokenSource: uint16(db.NetworkSolana),
			})
		}
	case [8]uint8{248, 198, 158, 145, 225, 117, 135, 200}:
		owner := ix.Accounts[3]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		solSwapEventFromTransfers(ctx, ix.InnerInstructions, app, "swap", db.EventTypeSwap)
	}
}
