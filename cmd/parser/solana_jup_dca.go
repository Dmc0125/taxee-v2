package parser

import (
	"encoding/binary"
	"slices"
	"taxee/pkg/assert"
	"taxee/pkg/db"

	"github.com/shopspring/decimal"
)

const SOL_JUP_DCA_PROGRAM_ADDRESS = "DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M"

var (
	jupDcaOpenV2 = [8]uint8{142, 119, 43, 109, 162, 52, 11, 177}
	jupDcaClose  = [8]uint8{83, 125, 166, 69, 247, 252, 103, 133}
)

func solPreprocessJupDcaIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	switch disc {
	case jupDcaOpenV2:
		owner := ix.Accounts[1]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		innerIxIter := solInnerIxIterator{innerIxs: ix.InnerInstructions}
		_, to, amount, err := solAnchorInitAccountValidate(&innerIxIter)
		assert.NoErr(err, "")

		ctx.receiveSol(to, amount)
		ctx.init(to, true, nil)

		// NOTE: not sure if it is possible for these create ATA ixs to not be
		// here, because the DCA account should get created here too and that
		// is the owner of both of the ATAs

		processCreateDcaAta := func() {
			if createAtaIx, ok := innerIxIter.peekNext(); ok && createAtaIx.ProgramAddress == SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS {
				innerIxIter.next()

				_, _, ata, mint, amount := solParseCreateAssociatedTokenAccount(
					createAtaIx.Accounts,
					&innerIxIter,
				)

				ctx.receiveSol(ata, amount)
				ctx.init(ata, true, &solTokenAccountData{
					Owner: owner,
					Mint:  mint,
				})
			}
		}

		// in ATA
		processCreateDcaAta()
		// out ATA
		processCreateDcaAta()
	case jupDcaClose:
		dcaAddress := ix.Accounts[1]
		if dcaAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, dcaAddress); dcaAccount == nil {
			return
		}

		inAta, outAta := ix.Accounts[4], ix.Accounts[5]
		ctx.close(inAta)
		ctx.close(outAta)
		ctx.close(dcaAddress)
	}
}

type jupDcaAccountData struct {
	InputMint    string
	InputAccount string
	Amount       decimal.Decimal
}

func (_ jupDcaAccountData) name() string {
	return "jup_dca"
}

func solProcessJupDcaIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "jupiter_dca"

	switch disc {
	case jupDcaOpenV2:
		owner := ix.Accounts[1]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		const method = "init_dca"

		innerIxIter := solInnerIxIterator{innerIxs: ix.InnerInstructions}
		_, err := solProcessAnchorInitAccount(
			ctx, &innerIxIter, owner, app, method,
		)
		assert.NoErr(err, "")

		processCreateDcaIx := func() {
			createAtaIx, ok := innerIxIter.peekNext()
			if !ok || createAtaIx.ProgramAddress != SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS {
				return
			}
			innerIxIter.next()

			payer, _, ata, _, amount := solParseCreateAssociatedTokenAccount(
				createAtaIx.Accounts, &innerIxIter,
			)

			event := solNewEvent(ctx, app, method, db.EventTypeTransfer)
			event.Transfers = append(event.Transfers, &db.EventTransfer{
				Direction:   db.EventTransferInternal,
				FromWallet:  payer,
				FromAccount: payer,
				ToWallet:    owner,
				ToAccount:   ata,
				Token:       SOL_MINT_ADDRESS,
				Amount:      newDecimalFromRawAmount(amount, 9),
				TokenSource: uint16(db.NetworkSolana),
			})
		}

		// in ata
		processCreateDcaIx()
		// out ata
		processCreateDcaIx()

		transferIx, ok := innerIxIter.nextSafe()
		if !ok {
			return
		}
		if transferIx.ProgramAddress == SOL_TOKEN_PROGRAM_ADDRESS {
			amount := binary.LittleEndian.Uint64(transferIx.Data[1:])
			from, to, mint := ix.Accounts[5], ix.Accounts[6], ix.Accounts[3]

			decimals, ok := solDecimals(ctx, mint)
			if !ok {
				return
			}

			event := solNewEvent(ctx, app, method, db.EventTypeTransfer)
			event.Transfers = append(event.Transfers, &db.EventTransfer{
				Direction:   db.EventTransferInternal,
				FromWallet:  owner,
				FromAccount: from,
				ToWallet:    owner,
				ToAccount:   to,
				Token:       mint,
				Amount:      newDecimalFromRawAmount(amount, decimals),
				TokenSource: uint16(db.NetworkSolana),
			})
		}
	// Init flash fill
	case [8]uint8{143, 205, 3, 191, 162, 215, 245, 49}:
		dcaAddress := ix.Accounts[1]
		dcaAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, dcaAddress)
		if dcaAccount == nil {
			return
		}

		transferIx := ix.InnerInstructions[0]
		t, ok := solParseTokenIxTokenTransfer(transferIx.Accounts, transferIx.Data)
		if !ok || t.ix != solTokenIxTransfer {
			return
		}

		_, fromAccountData, ok := solAccountExactOrError[solTokenAccountData](
			ctx, t.from,
		)
		if !ok {
			return
		}

		decimals, ok := ctx.tokensDecimals[fromAccountData.Mint]
		if !ok {
			return
		}

		dcaAccount.Data = &jupDcaAccountData{
			InputMint:    fromAccountData.Mint,
			InputAccount: t.from,
			Amount:       newDecimalFromRawAmount(t.amount, decimals),
		}
	// Transfer
	case [8]uint8{163, 52, 200, 231, 140, 3, 69, 186}:
		// TODO: Sol transfer
		dcaAddress := ix.Accounts[1]
		dcaAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, dcaAddress)
		if dcaAccount == nil {
			return
		}

		dcaAccountData, ok := solAccountDataMust[jupDcaAccountData](
			ctx, dcaAccount, dcaAddress,
		)
		if !ok {
			return
		}

		transferIx := ix.InnerInstructions[0]
		t, ok := solParseTokenIxTokenTransfer(transferIx.Accounts, transferIx.Data)
		if !ok || t.ix != solTokenIxTransfer {
			return
		}

		_, toAccountData, ok := solAccountExactOrError[solTokenAccountData](
			ctx, t.to,
		)
		if !ok {
			return
		}

		decimals, ok := solDecimals(ctx, toAccountData.Mint)
		if !ok {
			return
		}
		transfers := [2]*db.EventTransfer{
			{
				Direction:   db.EventTransferOutgoing,
				FromWallet:  toAccountData.Owner,
				FromAccount: dcaAccountData.InputAccount,
				Token:       dcaAccountData.InputMint,
				Amount:      dcaAccountData.Amount,
				TokenSource: uint16(db.NetworkSolana),
			},
			{
				Direction:   db.EventTransferIncoming,
				ToWallet:    toAccountData.Owner,
				ToAccount:   t.to,
				Token:       toAccountData.Mint,
				Amount:      newDecimalFromRawAmount(t.amount, decimals),
				TokenSource: uint16(db.NetworkSolana),
			},
		}

		event := solNewEvent(ctx, app, "swap", db.EventTypeSwap)
		event.Transfers = transfers[:]
	case jupDcaClose:
		dcaAddress := ix.Accounts[1]
		dcaAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, dcaAddress)
		if dcaAccount == nil {
			return
		}

		owner := ix.Accounts[6]
		newCloseEvent := func(account string) {
			event := solNewEvent(ctx, app, "close_dca", db.EventTypeCloseAccount)
			event.Transfers = append(event.Transfers, &db.EventTransfer{
				Direction:   db.EventTransferInternal,
				FromWallet:  owner,
				FromAccount: account,
				ToWallet:    owner,
				ToAccount:   owner,
				Token:       SOL_MINT_ADDRESS,
				TokenSource: uint16(db.NetworkSolana),
			})
		}

		// in ata
		newCloseEvent(ix.Accounts[4])
		// out ata
		newCloseEvent(ix.Accounts[5])
		newCloseEvent(dcaAddress)
	}
}
