package parser

import (
	"errors"
	"fmt"
	"slices"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"time"

	"github.com/shopspring/decimal"
)

type relatedAccounts interface {
	Append(string)
}

func RelatedAccountsFromTx(
	relatedAccounts relatedAccounts,
	walletAddress string,
	tx *db.SolanaTransactionData,
) {
	for _, ix := range tx.Instructions {
		switch ix.ProgramAddress {
		case SOL_TOKEN_PROGRAM_ADDRESS:
			solTokenIxRelatedAccounts(relatedAccounts, walletAddress, ix)
		case SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
			solAssociatedTokenIxRelatedAccounts(relatedAccounts, walletAddress, ix)
		case SOL_SQUADS_V4_PROGRAM_ADDRESS:
			solSquadsV4IxRelatedAccounts(relatedAccounts, walletAddress, ix)
		}
	}
}

type solAccount struct {
	balance uint64
	data    any
}

type solContext struct {
	wallets  []string
	accounts map[string]*solAccount
	errors   []*db.ParserError

	// volatile for each tx/ix
	txId           string
	slot           uint64
	timestamp      time.Time
	ixIdx          uint32
	tokensDecimals map[string]uint8
	nativeBalances map[string]*db.SolanaNativeBalance
	tokenBalances  map[string]*db.SolanaTokenBalances
}

func (ctx *solContext) walletOwned(other string) bool {
	return slices.Contains(ctx.wallets, other)
}

func solGetAccountData[T solAccountData](
	ctx *solContext,
	address string,
	result *T,
) bool {
	account, ok := ctx.accounts[address]
	if !ok {
		// TODO: ERROR
		return false
	}
	result, ok = account.data.(*T)
	if !ok {
		// TODO: ERROR
		return false
	}
	return true
}

func solGetAccountBalance(ctx *solContext, address string) (uint64, bool) {
	account, ok := ctx.accounts[address]
	if !ok {
		return 0, false
	}
	return account.balance, true
}

func solNewErrMissingAccount(ctx *solContext, address string) *db.ParserError {
	e := db.ParserError{
		TxId:  ctx.txId,
		IxIdx: int32(ctx.ixIdx),
		Type:  db.ParserErrorTypeMissingAccount,
		Data: &db.ParserErrorMissingAccount{
			AccountAddress: address,
		},
	}
	return &e
}

func solNewEvent(ctx *solContext) *db.Event {
	return &db.Event{
		Timestamp: ctx.timestamp,
		Network:   db.NetworkSolana,
		TxId:      ctx.txId,
		IxIdx:     int32(ctx.ixIdx),
	}
}

func solNewTxError(ctx *solContext) *db.ParserError {
	return &db.ParserError{
		TxId:  ctx.txId,
		IxIdx: int32(ctx.ixIdx),
	}
}

type solAccountData interface {
	name() string
}

func solAccountDataMust[T solAccountData](
	ctx *solContext,
	account *solAccount,
	address string,
) (*T, bool) {
	data, ok := account.data.(*T)
	if !ok {
		var temp T
		err := db.ParserError{
			TxId:  ctx.txId,
			IxIdx: int32(ctx.ixIdx),
			Type:  db.ParserErrorTypeAccountDataMismatch,
			Data: &db.ParserErrorAccountDataMismatch{
				AccountAddress: address,
				Message:        fmt.Sprintf("Expected the data to be %s", temp.name()),
			},
		}
		appendParserError(&ctx.errors, &err)
		return nil, false
	}
	return data, true
}

func solDecimalsMust(ctx *solContext, mint string) uint8 {
	// NOTE: this should **always** be ok, if it is not, RPC api changed and
	// we want to know that
	decimals, ok := ctx.tokensDecimals[mint]
	assert.True(ok, "missing decimals for: %s", mint)
	return decimals
}

func solAnchorDisc(data []byte) (disc [8]byte, ok bool) {
	if len(data) < 8 {
		return
	}

	copy(disc[:], data[:8])
	ok = true
	return
}

type solInnerIxIterator struct {
	innerIxs []*db.SolanaInnerInstruction
	pos      int
}

func (iter *solInnerIxIterator) hasNext() bool {
	return len(iter.innerIxs) > iter.pos
}

func (iter *solInnerIxIterator) next() *db.SolanaInnerInstruction {
	ix := iter.innerIxs[iter.pos]
	iter.pos += 1
	return ix
}

// NOTE: implemented based on
// https://github.com/solana-foundation/anchor/blob/master/lang/syn/src/codegen/accounts/constraints.rs#L1036
func solAnchorInitAccountValidate(
	innerIxs *solInnerIxIterator,
) (from, to string, amount uint64, err error) {
	if !innerIxs.hasNext() {
		err = errors.New("invalid anchor init ix: missing inner ixs")
		return
	}

	firstIx := innerIxs.next()
	if firstIx.ProgramAddress != SOL_SYSTEM_PROGRAM_ADDRESS {
		err = fmt.Errorf(
			"ivnalid anchor init ix: program address mismatch: %s",
			firstIx.ProgramAddress,
		)
		return
	}

	ixType, _, ok := solSystemIxFromData(firstIx.Data)
	if !ok {
		err = fmt.Errorf("invalid anchor init ix: %#v", *firstIx)
		return
	}

	switch ixType {
	case solSystemIxCreate:
	case solSystemIxTransfer:
		// NOTE: skip over allocate and assign
		innerIxs.next()
		if !innerIxs.hasNext() {
			err = errors.New("invalid anchor init ix")
			return
		}
		innerIxs.next()
	default:
		assert.True(false, "invalid anchor init ix: %d", ixType)
	}

	from, to, amount, ok = solParseSystemIxSolTransfer(
		ixType,
		firstIx.Accounts,
		firstIx.Data,
	)
	assert.True(ok, "invalid anchor init ix")

	return
}

// NOTE: implemented based on
// https://github.com/solana-foundation/anchor/blob/master/lang/syn/src/codegen/accounts/constraints.rs#L1036
func solProcessAnchorInitAccount(
	ctx *solContext,
	innerIxs *solInnerIxIterator,
) (*db.Event, bool, error) {
	from, to, amount, err := solAnchorInitAccountValidate(innerIxs)
	if err != nil {
		return nil, false, err
	}

	fromInternal, toInternal := ctx.walletOwned(from), ctx.walletOwned(to)
	if !fromInternal && !toInternal {
		return nil, false, nil
	}

	event := solNewEvent(ctx)
	setEventTransfer(
		event,
		from, to,
		from, to,
		fromInternal, toInternal,
		newDecimalFromRawAmount(amount, 9),
		SOL_MINT_ADDRESS,
		uint16(db.NetworkSolana),
	)

	return event, true, nil
}

func solParseTransfers(
	ctx *solContext,
	innerIxs []*db.SolanaInnerInstruction,
) (wallet string, incomingTransfers, outgoingTransfers []*db.EventSwapTransfer) {
	transfers := make(map[string]decimal.Decimal)
	tokenAccounts := make(map[string]string)

	// 1. token transfer - incoming / outgoing
	// 2. token mint - incoming
	// 3. token burn - outgoing

	getTokenAccount := func(address, mint string) (validMint, owner string, ok bool) {
		// TOOD:
		account := ctx.accounts[address]
		if account != nil {
			tokenAccountData, ok1 := solAccountDataMust[solTokenAccountData](
				ctx, account, address,
			)
			if !ok1 {
				return
			}
			if mint != "" && mint != tokenAccountData.Mint {
				// TODO: invalid mint error
				return
			}
			validMint = tokenAccountData.Mint
			owner = tokenAccountData.Owner
			ok = true
		}
		return
	}

	for _, innerIx := range innerIxs {
		switch innerIx.ProgramAddress {
		case SOL_TOKEN_PROGRAM_ADDRESS:
			ixType, _, ok := solTokenIxFromByte(innerIx.Data[0])
			if !ok {
				continue
			}

			var amount uint64
			var from, to, mint string

			switch ixType {
			case solTokenIxMint, solTokenIxMintChecked:
				amount, to, mint = solParseTokenMint(innerIx.Accounts, innerIx.Data)
			case solTokenIxBurn, solTokenIxBurnChecked:
				amount, from, mint = solParseTokenBurn(innerIx.Accounts, innerIx.Data)
			case solTokenIxTransfer:
				amount, from, to = solParseTokenTransfer(innerIx.Accounts, innerIx.Data)
			case solTokenIxTransferChecked:
				amount, from, to = solParseTokenTransferChecked(innerIx.Accounts, innerIx.Data)
			default:
				continue
			}

			validMint, ok := "", false
			if from != "" {
				if validMint, wallet, ok = getTokenAccount(from, mint); ok {
					decimals := solDecimalsMust(ctx, validMint)
					transfers[validMint] = transfers[validMint].Sub(
						newDecimalFromRawAmount(amount, decimals),
					)
					tokenAccounts[validMint] = from
				}
			}
			if to != "" {
				if validMint, wallet, ok = getTokenAccount(to, mint); ok {
					decimals := solDecimalsMust(ctx, validMint)
					transfers[validMint] = transfers[validMint].Add(
						newDecimalFromRawAmount(amount, decimals),
					)
					tokenAccounts[validMint] = to
				}
			}
		}
	}

	for mint, amount := range transfers {
		if amount.Equal(decimal.Zero) {
			continue
		}

		tokenAccount, ok := tokenAccounts[mint]
		assert.True(ok, "missing token account for mint")

		t := db.EventSwapTransfer{
			Account:     tokenAccount,
			Token:       mint,
			Amount:      amount.Abs(),
			TokenSource: uint16(db.NetworkSolana),
		}

		if amount.IsNegative() {
			outgoingTransfers = append(outgoingTransfers, &t)
		} else {
			incomingTransfers = append(incomingTransfers, &t)
		}
	}

	return
}

func solSwapEventFromTransfers(
	ctx *solContext,
	innerInstructions []*db.SolanaInnerInstruction,
	events *[]*db.Event,
	app, method string,
	eventType db.EventType,
) {
	owner, incomingTransfers, outgoingTransfers := solParseTransfers(ctx, innerInstructions)

	swapEventData := db.EventSwap{
		Wallet:   owner,
		Incoming: incomingTransfers,
		Outgoing: outgoingTransfers,
	}

	event := solNewEvent(ctx)
	event.UiAppName = app
	event.UiMethodName = method
	event.Type = eventType
	event.Data = &swapEventData

	*events = append(*events, event)
}

func solProcessIx(
	events *[]*db.Event,
	ctx *solContext,
	ix *db.SolanaInstruction,
) {
	switch ix.ProgramAddress {
	case SOL_SYSTEM_PROGRAM_ADDRESS:
		solProcessSystemIx(ctx, ix, events)
	case SOL_TOKEN_PROGRAM_ADDRESS:
		// solProcessTokenIx(ctx, ix, events)
	case SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
		solProcessAssociatedTokenIx(ctx, ix, events)
	case SOL_SQUADS_V4_PROGRAM_ADDRESS:
	// 	solProcessSquadsV4Ix(ctx, ix, events)
	case SOL_METEORA_POOLS_PROGRAM_ADDRESS:
		// solProcessMeteoraPoolsIx(ctx, ix, events)
	case SOL_JUP_V6_PROGRAM_ADDRESS:
		// solProcessJupV6(ctx, ix, events)
	case SOL_METEORA_FARMS_PROGRAM_ADDRESS:
		// solProcessMeteoraFarmsIx(ctx, ix, events)
	case SOL_MERCURIAL_PROGRAM_ADDRESS:
		// solProcessMercurialIx(ctx, ix, events)
	}
}
