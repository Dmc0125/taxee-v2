package parser

import (
	"encoding/binary"
	"slices"
	"taxee/pkg/db"
)

const SOL_JUP_V6_PROGRAM_ADDRESS = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

var (
	jupCreateTokenAccountIx = [8]uint8{147, 241, 123, 100, 244, 132, 174, 118}
)

func solJupV6RelatedAccounts(
	relatedAccounts relatedAccounts,
	walletAddress string,
	ix *db.SolanaInstruction,
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	if disc == jupCreateTokenAccountIx && len(ix.InnerInstructions) > 0 {
		owner, tokenAccount := ix.Accounts[1], ix.Accounts[0]
		if owner != walletAddress {
			return
		}
		relatedAccounts.Append(tokenAccount)
	}
}

func solPreprocessJupV6(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	if disc == jupCreateTokenAccountIx && len(ix.InnerInstructions) > 0 {
		owner, tokenAccount, mint := ix.Accounts[1], ix.Accounts[0], ix.Accounts[2]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		transferIx := ix.InnerInstructions[0]
		amount := binary.LittleEndian.Uint64(transferIx.Data[4:])
		ctx.receiveSol(tokenAccount, amount)
		ctx.init(tokenAccount, true, &solTokenAccountData{
			Mint:  mint,
			Owner: owner,
		})
	}
}

func solProcessJupV6(
	ctx *solContext,
	ix *db.SolanaInstruction,
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "jupiter"

	if disc == jupCreateTokenAccountIx && len(ix.InnerInstructions) > 0 {
		owner, tokenAccount := ix.Accounts[1], ix.Accounts[0]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		transferIx := ix.InnerInstructions[0]
		amount := binary.LittleEndian.Uint64(transferIx.Data[4:])

		event := solNewEvent(ctx, app, "create_token_account", db.EventTypeTransfer)
		event.Transfers = append(event.Transfers, &db.EventTransfer{
			Direction:   db.EventTransferInternal,
			FromWallet:  owner,
			FromAccount: owner,
			ToWallet:    owner,
			ToAccount:   tokenAccount,
			Token:       SOL_MINT_ADDRESS,
			Amount:      newDecimalFromRawAmount(amount, 9),
			TokenSource: uint16(db.NetworkSolana),
		})

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
	case [8]uint8{229, 23, 203, 151, 122, 227, 173, 42},
		// exact out route
		[8]uint8{208, 51, 239, 151, 123, 43, 237, 92},
		// shared accounts routed v2
		[8]uint8{209, 152, 83, 147, 124, 254, 216, 233}:
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
		app,
		"swap",
		db.EventTypeSwap,
	)
}
