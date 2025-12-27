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
			transfer, ok := solParseTokenIxTokenTransfer(transferIx.Accounts, transferIx.Data)
			if !ok || transfer.ix != solTokenIxTransfer {
				return
			}

			_, fromAccountData, ok := solAccountExactOrError[solTokenAccountData](
				ctx, transfer.from,
			)
			if !ok {
				return
			}

			decimals, ok := solDecimals(ctx, fromAccountData.Mint)
			if !ok {
				return
			}

			event := solNewEvent(ctx, app, "fee", db.EventTypeTransfer)
			event.Transfers = append(event.Transfers, &db.EventTransfer{
				Direction:   db.EventTransferOutgoing,
				FromWallet:  owner,
				FromAccount: transfer.from,
				Token:       fromAccountData.Mint,
				Amount:      newDecimalFromRawAmount(transfer.amount, decimals),
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
