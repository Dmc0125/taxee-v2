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

type solTokenAccountData struct {
	Mint  string
	Owner string
}

func (_ solTokenAccountData) name() string {
	return "token_account"
}

func solPreprocessTokenIx(ctx *solContext, ix *db.SolanaInstruction) {
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

	data := solTokenAccountData{
		Mint:  mint,
		Owner: owner,
	}
	ctx.init(tokenAccount, true, &data)
}

func solTokenProcessTransfer(
	ctx *solContext,
	from, to string,
	amount uint64,
) (d any, t db.EventType, ok bool) {
	fromAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, from)
	toAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, to)

	if fromAccount == nil && toAccount == nil {
		return
	}

	if fromAccount != nil && toAccount != nil {
		var fromAccountData, toAccountData *solTokenAccountData
		fromAccountData, ok = solAccountDataMust[solTokenAccountData](
			ctx, fromAccount, from,
		)
		if !ok {
			return
		}
		toAccountData, ok = solAccountDataMust[solTokenAccountData](
			ctx, toAccount, to,
		)
		if !ok {
			return
		}
		decimals := solDecimalsMust(ctx, fromAccountData.Mint)

		ok = true
		// TODO: check toAccount, it should have the same mint as fromAccount
		t = db.EventTypeTransfer
		d = &db.EventTransfer{
			Direction:   db.EventTransferInternal,
			FromWallet:  fromAccountData.Owner,
			ToWallet:    toAccountData.Owner,
			FromAccount: from,
			ToAccount:   to,
			Token:       fromAccountData.Mint,
			Amount:      newDecimalFromRawAmount(amount, decimals),
			TokenSource: uint16(db.NetworkSolana),
		}
		return
	}

	var tokenAccountData *solTokenAccountData
	event := db.EventTransfer{
		FromAccount: from,
		ToAccount:   to,
		TokenSource: uint16(db.NetworkSolana),
	}

	switch {
	case fromAccount != nil:
		tokenAccountData, ok = solAccountDataMust[solTokenAccountData](
			ctx, fromAccount, from,
		)
		if !ok {
			return
		}

		event.Direction = db.EventTransferOutgoing
		event.FromWallet = tokenAccountData.Owner
		event.Token = tokenAccountData.Mint
	case toAccount != nil:
		tokenAccountData, ok = solAccountDataMust[solTokenAccountData](
			ctx, toAccount, to,
		)
		if !ok {
			return
		}

		event.Direction = db.EventTransferIncoming
		event.ToWallet = tokenAccountData.Owner
		event.Token = tokenAccountData.Mint
	default:
		return
	}

	decimals := solDecimalsMust(ctx, tokenAccountData.Mint)
	event.Amount = newDecimalFromRawAmount(amount, decimals)

	ok = true
	t = db.EventTypeTransfer
	d = &event

	return
}

func solParseTokenTransfer(accounts []string, data []byte) (amount uint64, from, to string) {
	amount = binary.LittleEndian.Uint64(data[1:])
	from, to = accounts[0], accounts[1]
	return
}

func solParseTokenTransferChecked(accounts []string, data []byte) (amount uint64, from, to string) {
	amount = binary.LittleEndian.Uint64(data[1:])
	from, to = accounts[0], accounts[2]
	return
}

func solParseTokenMint(accounts []string, data []byte) (amount uint64, to, mint string) {
	mint, to = accounts[0], accounts[1]
	amount = binary.LittleEndian.Uint64(data[1:])
	return
}

func solParseTokenBurn(accounts []string, data []byte) (amount uint64, from, mint string) {
	from, mint = accounts[0], accounts[1]
	amount = binary.LittleEndian.Uint64(data[1:])
	return
}

func solProcessTokenIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	ixType, method, ok := solTokenIxFromByte(ix.Data[0])
	if !ok {
		return
	}

	var eventType db.EventType
	var eventData any

	switch ixType {
	case solTokenIxTransferChecked:
		amount, from, to := solParseTokenTransferChecked(ix.Accounts, ix.Data)
		if amount == 0 {
			return
		}

		eventData, eventType, ok = solTokenProcessTransfer(ctx, from, to, amount)
		if !ok {
			return
		}
	case solTokenIxTransfer:
		amount, from, to := solParseTokenTransfer(ix.Accounts, ix.Data)
		if amount == 0 {
			return
		}

		eventData, eventType, ok = solTokenProcessTransfer(ctx, from, to, amount)
		if !ok {
			return
		}
	case solTokenIxClose:
		return
	case solTokenIxMint, solTokenIxMintChecked:
		amount, to, mint := solParseTokenMint(ix.Accounts, ix.Data)
		if amount == 0 {
			return
		}

		receiverAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, to)
		if receiverAccount == nil {
			return
		}

		accountData, ok := solAccountDataMust[solTokenAccountData](
			ctx, receiverAccount, to,
		)
		if !ok {
			return
		}
		if accountData.Mint != mint {
			// TODO: error
			return
		}

		decimals := solDecimalsMust(ctx, accountData.Mint)
		eventType = db.EventTypeMint
		eventData = &db.EventTransfer{
			ToWallet:    accountData.Owner,
			ToAccount:   to,
			Token:       accountData.Mint,
			Amount:      newDecimalFromRawAmount(amount, decimals),
			TokenSource: uint16(db.NetworkSolana),
		}
	case solTokenIxBurn, solTokenIxBurnChecked:
		amount, from, mint := solParseTokenBurn(ix.Accounts, ix.Data)
		if amount == 0 {
			return
		}

		senderAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, from)
		if senderAccount == nil {
			return
		}

		accountData, ok := solAccountDataMust[solTokenAccountData](
			ctx, senderAccount, from,
		)
		if !ok {
			return
		}
		if accountData.Mint != mint {
			// TOOD: error
			return
		}

		decimals := solDecimalsMust(ctx, accountData.Mint)
		eventType = db.EventTypeBurn
		eventData = &db.EventTransfer{
			FromWallet:  accountData.Owner,
			FromAccount: from,
			Token:       accountData.Mint,
			Amount:      newDecimalFromRawAmount(amount, decimals),
			TokenSource: uint16(db.NetworkSolana),
		}
	default:
		return
	}

	event := solNewEvent(ctx)
	event.UiAppName = "token_program"
	event.UiMethodName = method
	event.Type = eventType
	event.Data = eventData

	*events = append(*events, event)
}
