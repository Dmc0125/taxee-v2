package parser

import (
	"encoding/binary"
	"taxee/pkg/db"

	"github.com/mr-tron/base58"
)

const SOL_TOKEN_PROGRAM_ADDRESS = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

type solTokenIx uint8

const (
	// Accounting
	solTokenIxInitializeAccount solTokenIx = iota
	solTokenIxInitializeAccount2
	solTokenIxInitializeAccount3
	solTokenIxClose

	// Economic
	solTokenIxTransfer solTokenIx = 50 + iota
)

func solTokenIxFromByte(disc byte) (ix solTokenIx, method string, ok bool) {
	ok = true

	switch solTokenIx(disc) {
	// Accounting
	case 1:
		ix = solTokenIxInitializeAccount
	case 16:
		ix = solTokenIxInitializeAccount2
	case 18:
		ix = solTokenIxInitializeAccount3
	case 9:
		ix, method = solTokenIxClose, "close_token_account"

	// Economic
	case 3:
		ix, method = solTokenIxTransfer, "transfer"
	default:
		ok = false
	}

	return
}

func solTokenIxRelatedAccounts(
	relatedAccounts relatedAccounts,
	walletAddress string,
	ix *db.SolanaInstruction,
) {
	ixType, _, ok := solTokenIxFromByte(ix.Data[0])
	if !ok || ixType > 50 {
		return
	}

	var owner, tokenAccount string

	switch ixType {
	case solTokenIxInitializeAccount:
		tokenAccount, owner = ix.Accounts[0], ix.Accounts[2]
	case solTokenIxInitializeAccount2, solTokenIxInitializeAccount3:
		tokenAccount = ix.Accounts[0]
		owner = base58.Encode(ix.Data[1:33])
	default:
		return
	}

	if walletAddress == owner {
		relatedAccounts.Append(tokenAccount)
		return
	}
}

type SolTokenAccountData struct {
	Mint  string
	Owner string
}

func solPreprocessTokenIx(ctx *solanaContext, ix *db.SolanaInstruction) {
	ixType, _, ok := solTokenIxFromByte(ix.Data[0])
	if !ok || ixType > 50 {
		return
	}

	var tokenAccount, mint, owner string

	switch ixType {
	case solTokenIxInitializeAccount:
		tokenAccount, mint, owner = ix.Accounts[0], ix.Accounts[1], ix.Accounts[2]
	case solTokenIxInitializeAccount3, solTokenIxInitializeAccount2:
		tokenAccount, mint = ix.Accounts[0], ix.Accounts[1]
		owner = base58.Encode(ix.Data[1:33])
	case solTokenIxClose:
		tokenAccount, owner = ix.Accounts[0], ix.Accounts[2]
		destination := ix.Accounts[1]

		if !ctx.walletOwned(owner) {
			return
		}

		ctx.close(tokenAccount, destination)
		return
	default:
		return
	}

	if !ctx.walletOwned(owner) {
		return
	}

	data := SolTokenAccountData{
		Mint:  mint,
		Owner: owner,
	}
	ctx.initOwned(tokenAccount, &data)
}

func solProcessTokenIx(
	ctx *solanaContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	ixType, method, ok := solTokenIxFromByte(ix.Data[0])
	if !ok {
		return
	}

	const app = "token_program"

	switch ixType {
	case solTokenIxTransfer:
		from, to := ix.Accounts[0], ix.Accounts[1]
		fromAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, from)
		toAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, to)

		if fromAccount == nil && toAccount == nil {
			return
		}

		event := db.Event{
			UiAppName:    app,
			UiMethodName: method,
			UiType:       db.UiEventTransfer,
			Transfers:    make([]*db.EventTransfer, 0),
		}
		ctx.initEvent(&event)
		amount := binary.LittleEndian.Uint64(ix.Data[1:])

		var transferOutgoing *db.EventTransfer
		var fromAccountData *SolTokenAccountData
		decimals := uint8(255)

		if fromAccount != nil {
			fromAccountData = solAccountDataMust[SolTokenAccountData](fromAccount)
			decimals = solDecimalsMust(ctx, fromAccountData.Mint)

			transferOutgoing = &db.EventTransfer{
				Type:    db.EventTransferOutgoing,
				Account: from,
				Token:   fromAccountData.Mint,
			}
			transferOutgoing.WithRawAmount(amount, decimals)

			event.Transfers = append(event.Transfers, transferOutgoing)
		}

		if toAccount != nil {
			transferIncoming := &db.EventTransfer{
				Type:    db.EventTransferIncoming,
				Account: to,
			}

			if fromAccountData != nil {
				transferOutgoing.Type |= db.EventTransferInternal

				transferIncoming.Type |= db.EventTransferInternal
				transferIncoming.Token = fromAccountData.Mint
				transferIncoming.WithRawAmount(amount, decimals)
			} else {
				toAccountData := solAccountDataMust[SolTokenAccountData](toAccount)
				decimals := solDecimalsMust(ctx, toAccountData.Mint)

				transferIncoming.Token = toAccountData.Mint
				transferIncoming.WithRawAmount(amount, decimals)
			}

			event.Transfers = append(event.Transfers, transferIncoming)
		}

		*events = append(*events, &event)
	case solTokenIxClose:
	default:
		return
	}
}
