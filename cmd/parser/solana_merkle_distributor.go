package parser

import (
	"slices"
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

const SOL_DRIFT_DISTRIBUTOR = "E7HtfkEMhmn9uwL7EFNydcXBWy5WCYN1vFmKKjipEH1x"

func solProcessMerkleDistributorIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
	app string,
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	if disc == [8]uint8{78, 177, 98, 123, 210, 21, 187, 83} {
		claimant := ix.Accounts[4]

		if !slices.Contains(ctx.wallets, claimant) {
			return
		}

		innerIxsIter := solInnerIxIterator{innerIxs: ix.InnerInstructions}

		// ix 1 -> create claim status
		ok, err := solProcessAnchorInitAccount(
			ctx,
			&innerIxsIter, claimant,
			app, "create_claim_status",
		)
		assert.NoErr(err, "")
		if !ok {
			return
		}

		// ix 2 -> claim
		claimIx := innerIxsIter.next()
		amount, _, to := solParseTokenTransfer(claimIx.Accounts, claimIx.Data)

		_, toAccountData, ok := solAccountExactOrError[solTokenAccountData](ctx, to)
		if !ok {
			return
		}

		decimals := solDecimalsMust(ctx, toAccountData.Mint)

		claimEvent := solNewEvent(ctx, app, "claim", db.EventTypeTransfer)
		claimEvent.Transfers = append(claimEvent.Transfers, &db.EventTransfer{
			Direction:   0,
			ToWallet:    claimant,
			ToAccount:   to,
			Token:       toAccountData.Mint,
			Amount:      newDecimalFromRawAmount(amount, decimals),
			TokenSource: uint16(db.NetworkSolana),
		})
	}
}
