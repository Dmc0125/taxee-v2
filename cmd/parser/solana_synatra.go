package parser

import (
	"slices"
	"taxee/pkg/db"

	"github.com/shopspring/decimal"
)

const SOL_SYNATRA_PROGRAM_ADDRESS = "synatfE5AvWtbDT9sSvDsF9gmeqR9qeq3FA84bhxWur"

var (
	synatraStakeSolIx   = [8]uint8{200, 38, 157, 155, 245, 57, 236, 168}
	synatraStakeTokenIx = [8]uint8{191, 127, 193, 101, 37, 96, 87, 211}
)

func solPreprocessSynatraIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	switch disc {
	case synatraStakeSolIx, synatraStakeTokenIx:
		owner := ix.Accounts[0]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		innerIxsIter := solInnerIxIterator{innerIxs: ix.InnerInstructions}
		if createAtaIx, ok := innerIxsIter.peekNext(); ok &&
			createAtaIx.ProgramAddress == SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS {
			innerIxsIter.next()
			_, owner, tokenAccount, mint, amount := solParseCreateAssociatedTokenAccount(
				createAtaIx.Accounts,
				&innerIxsIter,
			)

			ctx.receiveSol(tokenAccount, amount)
			ctx.init(tokenAccount, true, &solTokenAccountData{
				Mint:  mint,
				Owner: owner,
			})
		}
	}
}

func solProcessSynatraIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "synatra"

	if disc != synatraStakeSolIx && disc != synatraStakeTokenIx {
		return
	}

	owner := ix.Accounts[0]
	if !slices.Contains(ctx.wallets, owner) {
		return
	}

	innerIxs := solInnerIxIterator{innerIxs: ix.InnerInstructions}
	if createAtaIx, ok := innerIxs.peekNext(); ok &&
		createAtaIx.ProgramAddress == SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS {
		innerIxs.next()
		payer, _, tokenAccount, _, amount := solParseCreateAssociatedTokenAccount(
			createAtaIx.Accounts,
			&innerIxs,
		)

		event := solNewEvent(ctx, app, "create_account", db.EventTypeTransfer)
		event.Transfers = append(event.Transfers, &db.EventTransfer{
			Direction:   db.EventTransferInternal,
			FromWallet:  payer,
			FromAccount: payer,
			ToWallet:    owner,
			ToAccount:   tokenAccount,
			Token:       SOL_MINT_ADDRESS,
			Amount:      newDecimalFromRawAmount(amount, 9),
			TokenSource: uint16(db.NetworkSolana),
		})
	}

	var transferIn db.EventTransfer

	if transferInIx, ok := innerIxs.nextSafe(); ok {
		var fromAccount, mint string
		var amount decimal.Decimal

		switch disc {
		case synatraStakeSolIx:
			ixType, _, ok := solSystemIxFromData(transferInIx.Data)
			if !ok {
				return
			}
			from, _, rawAmount, ok := solParseSystemIxSolTransfer(
				ixType,
				transferInIx.Accounts,
				transferInIx.Data,
			)
			if !ok {
				return
			}
			fromAccount, mint = from, SOL_MINT_ADDRESS
			amount = newDecimalFromRawAmount(rawAmount, 9)
		case synatraStakeTokenIx:
			ixType, _, ok := solTokenIxFromByte(transferInIx.Data[0])
			if !ok || ixType != solTokenIxTransfer {
				return
			}
			rawAmount, from, _ := solParseTokenTransfer(
				transferInIx.Accounts,
				transferInIx.Data,
			)

			fromAccount = from
			mint = ix.Accounts[2]
			decimals := solDecimalsMust(ctx, mint)
			amount = newDecimalFromRawAmount(rawAmount, decimals)
		}

		transferIn = db.EventTransfer{
			Direction:   db.EventTransferOutgoing,
			FromWallet:  owner,
			FromAccount: fromAccount,
			Token:       mint,
			Amount:      amount,
			TokenSource: uint16(db.NetworkSolana),
		}
	} else {
		return
	}

	if mintIx, ok := innerIxs.nextSafe(); ok {
		ixType, _, ok := solTokenIxFromByte(mintIx.Data[0])
		if !ok || ixType != solTokenIxMint {
			return
		}
		amount, to, mint := solParseTokenMint(mintIx.Accounts, mintIx.Data)
		decimals := solDecimalsMust(ctx, mint)

		ysolTransfer := db.EventTransfer{
			Direction:   db.EventTransferIncoming,
			ToWallet:    owner,
			ToAccount:   to,
			Token:       mint,
			Amount:      newDecimalFromRawAmount(amount, decimals),
			TokenSource: uint16(db.NetworkSolana),
		}

		event := solNewEvent(ctx, app, "swap", db.EventTypeSwap)
		event.Transfers = append(event.Transfers, &transferIn, &ysolTransfer)
	}
}
