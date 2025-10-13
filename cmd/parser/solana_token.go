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
	solTokenIxEconomicBr
	solTokenIxTransfer
	solTokenIxTransferChecked
	solTokenIxMint
	solTokenIxMintChecked
	solTokenIxBurn
	solTokenIxBurnChecked
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
	case 12:
		ix, method = solTokenIxTransferChecked, "transfer_checked"
	case 7:
		ix, method = solTokenIxMint, "mint"
	case 14:
		ix, method = solTokenIxMintChecked, "mint_checked"
	case 8:
		ix, method = solTokenIxBurn, "burn"
	case 15:
		ix, method = solTokenIxBurnChecked, "burn_checked"
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
	if !ok || ixType > solTokenIxEconomicBr {
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
	if !ok || ixType > solTokenIxEconomicBr {
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
	ctx.init(tokenAccount, true, &data)
}

func solTokenProcessTransfer(
	ctx *solanaContext,
	from, to string,
	amount uint64,
) (d db.EventData, t db.EventType, ok bool) {
	fromAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, from)
	toAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, to)

	if fromAccount == nil && toAccount == nil {
		return
	}

	if fromAccount != nil && toAccount != nil {
		fromAccountData := solAccountDataMust[SolTokenAccountData](fromAccount)
		decimals := solDecimalsMust(ctx, fromAccountData.Mint)

		ok = true
		// TODO: check toAccount, it should have the same mint as fromAccount
		t = db.EventTypeTransferInternal
		d = &db.EventTransferInternal{
			FromAccount: from,
			ToAccount:   to,
			Token:       fromAccountData.Mint,
			Amount:      newDecimalFromRawAmount(amount, decimals),
		}
		return
	}

	var tokenAccountData *SolTokenAccountData
	var tokenAccount string
	var direction db.EventTransferDirection

	switch {
	case fromAccount != nil:
		tokenAccountData = solAccountDataMust[SolTokenAccountData](fromAccount)
		direction, tokenAccount = db.EventTransferOutgoing, from
	case toAccount != nil:
		tokenAccountData = solAccountDataMust[SolTokenAccountData](toAccount)
		direction, tokenAccount = db.EventTransferIncoming, to
	default:
		return
	}

	decimals := solDecimalsMust(ctx, tokenAccountData.Mint)
	ok = true
	t = db.EventTypeTransfer
	d = &db.EventTransfer{
		Direction: direction,
		Account:   tokenAccount,
		Token:     tokenAccountData.Mint,
		Amount:    newDecimalFromRawAmount(amount, decimals),
	}

	return
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
	case solTokenIxTransferChecked:
		amount := binary.LittleEndian.Uint64(ix.Data[1:])
		if amount == 0 {
			return
		}
		from, to := ix.Accounts[0], ix.Accounts[2]

		eventData, eventType, ok := solTokenProcessTransfer(ctx, from, to, amount)
		if !ok {
			return
		}

		event := db.Event{
			UiAppName:    app,
			UiMethodName: method,
			Type:         eventType,
			Data:         eventData,
		}
		ctx.initEvent(&event)

		*events = append(*events, &event)
	case solTokenIxTransfer:
		amount := binary.LittleEndian.Uint64(ix.Data[1:])
		if amount == 0 {
			return
		}
		from, to := ix.Accounts[0], ix.Accounts[1]

		eventData, eventType, ok := solTokenProcessTransfer(ctx, from, to, amount)
		if !ok {
			return
		}

		event := db.Event{
			UiAppName:    app,
			UiMethodName: method,
			Type:         eventType,
			Data:         eventData,
		}
		ctx.initEvent(&event)

		*events = append(*events, &event)
	case solTokenIxClose:
	case solTokenIxMint, solTokenIxMintChecked:
		mint, receiver := ix.Accounts[0], ix.Accounts[1]
		amount := binary.LittleEndian.Uint64(ix.Data[1:])
		if amount == 0 {
			return
		}

		receiverAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, receiver)
		if receiverAccount == nil {
			return
		}

		accountData := solAccountDataMust[SolTokenAccountData](receiverAccount)
		if accountData.Mint != mint {
			// TODO: error
			return
		}

		decimals := solDecimalsMust(ctx, accountData.Mint)
		event := db.Event{
			UiAppName:    app,
			UiMethodName: method,
			Type:         db.EventTypeMint,
			Data: &db.EventTransfer{
				Account: receiver,
				Token:   accountData.Mint,
				Amount:  newDecimalFromRawAmount(amount, decimals),
			},
		}
		ctx.initEvent(&event)

		*events = append(*events, &event)
	case solTokenIxBurn, solTokenIxBurnChecked:
		sender, mint := ix.Accounts[0], ix.Accounts[1]
		amount := binary.LittleEndian.Uint64(ix.Data[1:])
		if amount == 0 {
			return
		}

		senderAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, sender)
		if senderAccount == nil {
			return
		}

		accountData := solAccountDataMust[SolTokenAccountData](senderAccount)
		if accountData.Mint != mint {
			// TOOD: error
			return
		}

		decimals := solDecimalsMust(ctx, accountData.Mint)
		event := db.Event{
			UiAppName:    app,
			UiMethodName: method,
			Type:         db.EventTypeBurn,
			Data: &db.EventTransfer{
				Account: sender,
				Token:   accountData.Mint,
				Amount:  newDecimalFromRawAmount(amount, decimals),
			},
		}
		ctx.initEvent(&event)

		*events = append(*events, &event)
	default:

		return
	}
}
