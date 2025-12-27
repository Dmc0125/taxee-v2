package parser

import (
	"encoding/binary"
	"taxee/pkg/db"

	"github.com/mr-tron/base58"
	"github.com/shopspring/decimal"
)

const SOL_TOKEN_PROGRAM_ADDRESS = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
const SOL_TOKEN2022_PROGRAM_ADDRESS = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

type solTokenIx uint8

func (ix solTokenIx) economic() bool {
	return ix > solTokenIxEconomicBr
}

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

type solTokenTransfer struct {
	ix     solTokenIx
	method string
	from   string
	to     string
	mint   string
	amount uint64
}

func solParseTokenIxTokenTransfer(
	accounts []string,
	data []byte,
) (transfer *solTokenTransfer, ok bool) {
	if len(data) < 1 {
		return
	}

	transfer = new(solTokenTransfer)
	ok = true

	parseMint := func() {
		transfer.mint, transfer.to = accounts[0], accounts[1]
		transfer.amount = binary.LittleEndian.Uint64(data[1:])
	}

	parseBurn := func() {
		transfer.from, transfer.mint = accounts[0], accounts[1]
		transfer.amount = binary.LittleEndian.Uint64(data[1:])
	}

	// TODO: safety
	switch data[0] {
	// Accounting
	case 1:
		transfer.ix = solTokenIxInitializeAccount
	case 16:
		transfer.ix = solTokenIxInitializeAccount2
	case 18:
		transfer.ix = solTokenIxInitializeAccount3
	case 9:
		transfer.ix = solTokenIxClose
		transfer.method = "close_account"

	// Economic
	case 3:
		transfer.ix = solTokenIxTransfer
		transfer.method = "transfer"

		transfer.amount = binary.LittleEndian.Uint64(data[1:])
		transfer.from, transfer.to = accounts[0], accounts[1]
	case 12:
		transfer.ix = solTokenIxTransferChecked
		transfer.method = "transfer"

		transfer.amount = binary.LittleEndian.Uint64(data[1:])
		transfer.from, transfer.to = accounts[0], accounts[2]
	case 7:
		transfer.ix = solTokenIxMint
		transfer.method = "mint"
		parseMint()
	case 14:
		transfer.ix = solTokenIxMintChecked
		transfer.method = "mint"
		parseMint()
	case 8:
		transfer.ix = solTokenIxBurn
		transfer.method = "burn"
		parseBurn()
	case 15:
		transfer.ix = solTokenIxBurnChecked
		transfer.method = "burn"
		parseBurn()
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
	transfer, ok := solParseTokenIxTokenTransfer(ix.Accounts, ix.Data)
	if !ok || transfer.ix > solTokenIxEconomicBr {
		return
	}

	var owner, tokenAccount string

	switch transfer.ix {
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
	transfer, ok := solParseTokenIxTokenTransfer(ix.Accounts, ix.Data)
	if !ok || transfer.ix > solTokenIxEconomicBr {
		return
	}

	var tokenAccount, mint, owner string

	switch transfer.ix {
	case solTokenIxInitializeAccount:
		tokenAccount, mint, owner = ix.Accounts[0], ix.Accounts[1], ix.Accounts[2]
	case solTokenIxInitializeAccount3, solTokenIxInitializeAccount2:
		tokenAccount, mint = ix.Accounts[0], ix.Accounts[1]
		owner = base58.Encode(ix.Data[1:33])
	case solTokenIxClose:
		tokenAccount, owner = ix.Accounts[0], ix.Accounts[2]
		if !ctx.walletOwned(owner) {
			return
		}

		ctx.close(tokenAccount)
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

func solTokenProcessTransfer(ctx *solContext, t *solTokenTransfer) (*db.EventTransfer, bool) {
	fromAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, t.from)
	toAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, t.to)

	if fromAccount == nil && toAccount == nil {
		return nil, false
	}

	if fromAccount != nil && toAccount != nil {
		fromAccountData, ok := solAccountDataMust[solTokenAccountData](
			ctx, fromAccount, t.from,
		)
		if !ok {
			return nil, false
		}
		toAccountData, ok := solAccountDataMust[solTokenAccountData](
			ctx, toAccount, t.to,
		)
		if !ok {
			return nil, false
		}
		decimals, ok := solDecimals(ctx, fromAccountData.Mint)
		if !ok {
			return nil, false
		}

		// TODO: check toAccount, it should have the same mint as fromAccount
		transfer := &db.EventTransfer{
			Direction:   db.EventTransferInternal,
			FromWallet:  fromAccountData.Owner,
			ToWallet:    toAccountData.Owner,
			FromAccount: t.from,
			ToAccount:   t.to,
			Token:       fromAccountData.Mint,
			Amount:      newDecimalFromRawAmount(t.amount, decimals),
			TokenSource: uint16(db.NetworkSolana),
		}
		return transfer, true
	}

	var tokenAccountData *solTokenAccountData
	var ok bool
	transfer := db.EventTransfer{
		FromAccount: t.from,
		ToAccount:   t.to,
		TokenSource: uint16(db.NetworkSolana),
	}

	switch {
	case fromAccount != nil:
		tokenAccountData, ok = solAccountDataMust[solTokenAccountData](
			ctx, fromAccount, t.from,
		)
		if !ok {
			return nil, false
		}

		transfer.Direction = db.EventTransferOutgoing
		transfer.FromWallet = tokenAccountData.Owner
		transfer.Token = tokenAccountData.Mint
	case toAccount != nil:
		tokenAccountData, ok = solAccountDataMust[solTokenAccountData](
			ctx, toAccount, t.to,
		)
		if !ok {
			return nil, false
		}

		transfer.Direction = db.EventTransferIncoming
		transfer.ToWallet = tokenAccountData.Owner
		transfer.Token = tokenAccountData.Mint
	default:
		return nil, false
	}

	decimals, ok := solDecimals(ctx, tokenAccountData.Mint)
	if !ok {
		return nil, false
	}
	transfer.Amount = newDecimalFromRawAmount(t.amount, decimals)

	return &transfer, true
}

func solProcessTokenIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
) {
	t, ok := solParseTokenIxTokenTransfer(ix.Accounts, ix.Data)
	if !ok {
		return
	}

	var eventType db.EventType
	var transfer *db.EventTransfer

	processMintBurn := func(
		tokenAccountAddress, mint string,
		rawAmount uint64,
	) (owner string, amount decimal.Decimal, ok bool) {
		tokenAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, tokenAccountAddress)
		if tokenAccount == nil {
			return
		}

		tokenAccountData, ok := solAccountDataMust[solTokenAccountData](
			ctx, tokenAccount, tokenAccountAddress,
		)
		if !ok {
			return
		}
		if tokenAccountData.Mint != mint {
			// TOOD: error
			return
		}

		ok = true
		owner = tokenAccountData.Owner
		decimals, ok := ctx.tokensDecimals[mint]
		if !ok {
			return
		}
		amount = newDecimalFromRawAmount(rawAmount, decimals)
		return
	}

	switch t.ix {
	case solTokenIxTransferChecked, solTokenIxTransfer:
		if transfer, ok = solTokenProcessTransfer(ctx, t); !ok {
			return
		}
		eventType = db.EventTypeTransfer
	case solTokenIxMint, solTokenIxMintChecked:
		owner, amount, ok := processMintBurn(t.to, t.mint, t.amount)
		if !ok {
			return
		}

		eventType = db.EventTypeMint
		transfer = &db.EventTransfer{
			Direction:   db.EventTransferIncoming,
			ToWallet:    owner,
			ToAccount:   t.to,
			Token:       t.mint,
			Amount:      amount,
			TokenSource: uint16(db.NetworkSolana),
		}
	case solTokenIxBurn, solTokenIxBurnChecked:
		owner, amount, ok := processMintBurn(t.from, t.mint, t.amount)
		if !ok {
			return
		}

		eventType = db.EventTypeBurn
		transfer = &db.EventTransfer{
			Direction:   db.EventTransferOutgoing,
			FromWallet:  owner,
			FromAccount: t.from,
			Token:       t.mint,
			Amount:      amount,
			TokenSource: uint16(db.NetworkSolana),
		}
	case solTokenIxClose:
		closedAccountAddress, destinationAddress := ix.Accounts[0], ix.Accounts[1]

		closedAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, closedAccountAddress)
		destinationAccountInternal := ctx.walletOwned(destinationAddress)

		if closedAccount == nil && !destinationAccountInternal {
			return
		}

		if closedAccount == nil && destinationAccountInternal {
			// TODO: display some warning in the UI that this is a close ix
			// but there is no way for us to know the amount and the user has
			// to create the event
			return
		}

		closedAccountData, ok := solAccountDataMust[solTokenAccountData](
			ctx, closedAccount, closedAccountAddress,
		)
		if !ok {
			return
		}

		eventType = db.EventTypeCloseAccount
		transfer = &db.EventTransfer{
			Direction:   getTransferEventDirection(true, destinationAccountInternal),
			FromWallet:  closedAccountData.Owner,
			ToWallet:    destinationAddress,
			FromAccount: closedAccountAddress,
			ToAccount:   destinationAddress,
			Token:       SOL_MINT_ADDRESS,
			TokenSource: uint16(db.NetworkSolana),
		}
	default:
		return
	}

	event := solNewEvent(ctx, "token", t.method, eventType)
	event.Transfers = append(event.Transfers, transfer)
}
