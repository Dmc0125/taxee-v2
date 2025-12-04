package parser

import (
	"fmt"
	"slices"
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

const SOL_LULO_PROGRAM_ADDRESS = "FL3X2pRsQ9zHENpZSKDRREtccwJuei8yg9fwDu9UN69Q"

var (
	luloInitiateRegularWithdrawIx = [8]uint8{163, 41, 200, 57, 143, 231, 232, 12}
	luloCompleteRegularWithdrawIx = [8]uint8{65, 156, 75, 124, 82, 64, 245, 159}
)

func solPreprocessLuloIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	switch disc {
	case luloInitiateRegularWithdrawIx:
		owner := ix.Accounts[0]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		err := solPreprocessAnchorInitAccount(
			ctx,
			owner,
			ix.InnerInstructions,
		)
		assert.NoErr(err, "")
	case luloCompleteRegularWithdrawIx:
		owner, withdrawAccount := ix.Accounts[0], ix.Accounts[3]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		ctx.close(withdrawAccount)
	}
}

func solProcessLuloIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const (
		app              = "lulo"
		accountProtected = "protected"
		accountBoosted   = "boosted"
		accountRegular   = "regular"
	)

	if disc == luloInitiateRegularWithdrawIx {
		innerIxs := solInnerIxIterator{innerIxs: ix.InnerInstructions}
		_, err := solProcessAnchorInitAccount(
			ctx, &innerIxs,
			ix.Accounts[0], app, "initiate_withdraw",
		)
		assert.NoErr(err, "")
		return
	}

	deposit := func() (method, accountType string, ok bool) {
		switch disc {
		// deposit protected
		case [8]uint8{183, 216, 75, 226, 254, 144, 212, 7}:
			method, accountType = "deposit_protected", accountProtected
			ok = true
		// deposit boosted
		case [8]uint8{119, 176, 237, 141, 224, 45, 231, 59}:
			method, accountType = "deposit_boosted", accountBoosted
			ok = true
		// deposit regular
		case [8]uint8{186, 198, 140, 233, 129, 39, 98, 153}:
			method, accountType = "deposit_regular", accountRegular
			ok = true
		}
		return
	}

	if method, luloAccountType, ok := deposit(); ok {
		innerIxs := solInnerIxIterator{innerIxs: ix.InnerInstructions}

		if createAtaIx, ok := innerIxs.peekNext(); ok &&
			createAtaIx.ProgramAddress == SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS {
			innerIxs.next()
			payer, _, _, _, amount := solParseCreateAssociatedTokenAccount(
				createAtaIx.Accounts,
				&innerIxs,
			)

			if slices.Contains(ctx.wallets, payer) {
				event := solNewEvent(ctx, app, "create_token_account", db.EventTypeTransfer)
				event.Transfers = append(event.Transfers, &db.EventTransfer{
					Direction:   db.EventTransferOutgoing,
					FromWallet:  payer,
					FromAccount: payer,
					Token:       SOL_MINT_ADDRESS,
					Amount:      newDecimalFromRawAmount(amount, 9),
					TokenSource: uint16(db.NetworkSolana),
				})
			}
		}

		owner := ix.Accounts[0]

		if createLuloAccountIx, ok := innerIxs.peekNext(); ok &&
			createLuloAccountIx.ProgramAddress == SOL_LULO_PROGRAM_ADDRESS {
			innerIxs.next()

			ok, err := solProcessAnchorInitAccount(
				ctx, &innerIxs,
				owner, app, "create_lulo_account",
			)
			assert.NoErr(err, "")
			if !ok {
				return
			}
		}

		if feeIx, ok := innerIxs.peekNext(); ok &&
			feeIx.ProgramAddress == SOL_SYSTEM_PROGRAM_ADDRESS {
			innerIxs.next()

			ixType, _, ok := solSystemIxFromData(feeIx.Data)
			if !ok {
				return
			}
			from, _, amount, ok := solParseSystemIxSolTransfer(
				ixType,
				feeIx.Accounts,
				feeIx.Data,
			)
			if !ok {
				return
			}
			if slices.Contains(ctx.wallets, from) {
				event := solNewEvent(ctx, app, "fee", db.EventTypeTransfer)
				event.Transfers = append(event.Transfers, &db.EventTransfer{
					Direction:   db.EventTransferOutgoing,
					FromWallet:  from,
					FromAccount: from,
					Token:       SOL_MINT_ADDRESS,
					Amount:      newDecimalFromRawAmount(amount, 9),
					TokenSource: uint16(db.NetworkSolana),
				})
			}
		}

		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		// mint ix
		// it is not required that this ix exists, so only skip if it does
		if mintIx, ok := innerIxs.peekNext(); ok {
			switch mintIx.ProgramAddress {
			case SOL_TOKEN_PROGRAM_ADDRESS, SOL_TOKEN2022_PROGRAM_ADDRESS:
				ixType, _, ok := solTokenIxFromByte(mintIx.Data[0])
				if ok && ixType == solTokenIxMint {
					innerIxs.next()
				}
			}
		} else {
			return
		}

		if transferIx, ok := innerIxs.nextSafe(); ok {
			ixType, _, ok := solTokenIxFromByte(transferIx.Data[0])
			if !ok {
				return
			}

			var from string
			var amount uint64

			switch ixType {
			case solTokenIxTransfer:
				amount, from, _ = solParseTokenTransfer(transferIx.Accounts, transferIx.Data)
			case solTokenIxTransferChecked:
				amount, from, _ = solParseTokenTransferChecked(transferIx.Accounts, transferIx.Data)
			default:
				return
			}

			_, fromAccountData, ok := solAccountExactOrError[solTokenAccountData](ctx, from)
			if !ok {
				return
			}

			decimals := solDecimalsMust(ctx, fromAccountData.Mint)
			// luloAccount := ix.Accounts[8]

			event := solNewEvent(ctx, app, method, db.EventTypeTransfer)
			event.Transfers = append(event.Transfers, &db.EventTransfer{
				Direction:   db.EventTransferInternal,
				FromWallet:  owner,
				FromAccount: from,
				// TODO: decide what the to account is in this case ?
				// think the created token account could be it
				ToWallet:    owner,
				ToAccount:   fmt.Sprintf("%s:lulo:%s", owner, luloAccountType),
				Token:       fromAccountData.Mint,
				Amount:      newDecimalFromRawAmount(amount, decimals),
				TokenSource: uint16(db.NetworkSolana),
			})
		}

		return
	}

	newWithdrawEvent := func(owner, method, luloAccountType string, innerIxs *solInnerIxIterator) {
		if transferIx, ok := innerIxs.nextSafe(); ok &&
			transferIx.ProgramAddress == SOL_TOKEN_PROGRAM_ADDRESS {
			ixType, _, ok := solTokenIxFromByte(transferIx.Data[0])
			if !ok || ixType != solTokenIxTransferChecked {
				return
			}

			amount, _, to := solParseTokenTransferChecked(
				transferIx.Accounts,
				transferIx.Data,
			)

			_, tokenAccountData, ok := solAccountExactOrError[solTokenAccountData](
				ctx, to,
			)
			if !ok {
				return
			}

			decimals := solDecimalsMust(ctx, tokenAccountData.Mint)

			event := solNewEvent(ctx, app, method, db.EventTypeTransfer)
			event.Transfers = append(event.Transfers, &db.EventTransfer{
				Direction:   db.EventTransferInternal,
				FromWallet:  owner,
				FromAccount: fmt.Sprintf("%s:lulo:%s", owner, luloAccountType),
				ToWallet:    owner,
				ToAccount:   to,
				Token:       tokenAccountData.Mint,
				Amount:      newDecimalFromRawAmount(amount, decimals),
				TokenSource: uint16(db.NetworkSolana),
			})
		}
	}

	switch disc {
	// withdraw protected
	case [8]uint8{109, 247, 20, 102, 254, 61, 118, 108}:
		owner := ix.Accounts[0]
		if !slices.Contains(ctx.wallets, owner) || len(ix.InnerInstructions) < 2 {
			return
		}

		innerIxs := &solInnerIxIterator{
			innerIxs: ix.InnerInstructions,
			pos:      len(ix.InnerInstructions) - 2,
		}
		newWithdrawEvent(owner, "withdraw_protected", accountProtected, innerIxs)
	case luloCompleteRegularWithdrawIx:
		owner, withdrawAccount := ix.Accounts[0], ix.Accounts[3]
		if !slices.Contains(ctx.wallets, owner) || len(ix.InnerInstructions) == 0 {
			return
		}

		innerIxs := &solInnerIxIterator{
			innerIxs: ix.InnerInstructions,
			pos:      len(ix.InnerInstructions) - 1,
		}
		newWithdrawEvent(owner, "withdraw_boosted", accountBoosted, innerIxs)

		closeEvent := solNewEvent(ctx, app, "close_withdraw_account", db.EventTypeCloseAccount)
		closeEvent.Transfers = append(closeEvent.Transfers, &db.EventTransfer{
			Direction:   db.EventTransferInternal,
			FromWallet:  owner,
			FromAccount: withdrawAccount,
			ToWallet:    owner,
			ToAccount:   owner,
			Token:       SOL_MINT_ADDRESS,
			TokenSource: uint16(db.NetworkSolana),
		})
	}
}
