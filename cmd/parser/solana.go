package parser

import (
	"errors"
	"fmt"
	"math"
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

type accountLifetime struct {
	MinSlot  uint64
	MaxSlot  uint64
	MinIxIdx uint32
	MaxIxIdx uint32
	// used in preparse to know if an account is still
	// open inside one instruction
	PreparseOpen bool
	Owned        bool
	Data         any
	Balance      uint64
}

func newAccountLifetime(slot uint64, ixIdx uint32) accountLifetime {
	return accountLifetime{
		MinSlot:      slot,
		MinIxIdx:     ixIdx,
		MaxSlot:      math.MaxUint64,
		MaxIxIdx:     math.MaxUint32,
		PreparseOpen: true,
	}
}

func (acc *accountLifetime) open(slot uint64, ixIdx uint32) bool {
	if acc.MinSlot == slot && acc.MinIxIdx < ixIdx {
		return true
	}
	if acc.MaxSlot == slot && acc.MaxIxIdx > ixIdx {
		return true
	}
	return acc.MinSlot < slot && slot < acc.MaxSlot
}

type solContext struct {
	wallets  []string
	accounts map[string][]accountLifetime

	errors []*db.ParserError

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

func (ctx *solContext) receiveSol(address string, amount uint64) {
	lifetimes, ok := ctx.accounts[address]

	if !ok || len(lifetimes) == 0 {
		acc := newAccountLifetime(ctx.slot, ctx.ixIdx)
		acc.Balance = amount
		lifetimes = append(lifetimes, acc)
		ctx.accounts[address] = lifetimes
		return
	}

	account := &lifetimes[len(lifetimes)-1]

	if account.PreparseOpen {
		account.Balance += amount
	} else {
		acc := newAccountLifetime(ctx.slot, ctx.ixIdx)
		acc.Balance = amount
		lifetimes = append(lifetimes, acc)
	}

	ctx.accounts[address] = lifetimes
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

func (ctx *solContext) init(address string, owned bool, data any) {
	lifetimes, ok := ctx.accounts[address]
	if !ok || len(lifetimes) == 0 {
		err := solNewErrMissingAccount(ctx, address)
		ctx.errors = append(ctx.errors, err)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		err := solNewErrMissingAccount(ctx, address)
		ctx.errors = append(ctx.errors, err)
		return
	}

	acc.Data = data
	acc.Owned = owned
	ctx.accounts[address] = lifetimes
}

func (ctx *solContext) close(closedAddress, receiverAddress string) {
	lifetimes, ok := ctx.accounts[closedAddress]
	if !ok || len(lifetimes) == 0 {
		err := solNewErrMissingAccount(ctx, closedAddress)
		ctx.errors = append(ctx.errors, err)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		err := solNewErrMissingAccount(ctx, closedAddress)
		ctx.errors = append(ctx.errors, err)
		return
	}

	acc.PreparseOpen = false
	acc.MaxSlot = ctx.slot
	acc.MaxIxIdx = ctx.ixIdx
	ctx.accounts[closedAddress] = lifetimes

	ctx.receiveSol(receiverAddress, acc.Balance)
}

// 0 -> not owned, 1 -> owned, 2 -> whatever
func (ctx *solContext) find(
	slot uint64,
	ixIdx uint32,
	address string,
	owned uint8,
) *accountLifetime {
	isValid := func(lt *accountLifetime, o uint8) bool {
		return o == 2 ||
			(o == 1 && lt.Owned) ||
			(o == 0 && !lt.Owned)
	}

	lifetimes, ok := ctx.accounts[address]
	if !ok || len(lifetimes) == 0 {
		return nil
	}

	for i := 0; i < len(lifetimes); i += 1 {
		lt := &lifetimes[i]
		if lt.open(slot, ixIdx) && isValid(lt, owned) {
			return lt
		}
	}

	return nil
}

func (ctx *solContext) findOwned(
	slot uint64,
	ixIdx uint32,
	address string,
) *accountLifetime {
	return ctx.find(slot, ixIdx, address, 1)
}

func (ctx *solContext) findOwnedOrError(
	slot uint64,
	ixIdx uint32,
	address string,
) (account *accountLifetime, ok bool) {
	account = ctx.find(slot, ixIdx, address, 1)
	if account == nil {
		err := solNewErrMissingAccount(ctx, address)
		ctx.errors = append(ctx.errors, err)
	} else {
		ok = true
	}
	return
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

// TODO: instead of assert, create an error
func solAccountDataMust[T solAccountData](
	ctx *solContext,
	account *accountLifetime,
	address string,
) (*T, bool) {
	data, ok := account.Data.(*T)
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
		ctx.errors = append(ctx.errors, &err)
		return nil, false
	}
	return data, true
}

func solAccountExactOrError[T solAccountData](
	ctx *solContext,
	address string,
) (*accountLifetime, *T, bool) {
	account, ok := ctx.findOwnedOrError(ctx.slot, ctx.ixIdx, address)
	if !ok {
		return nil, nil, false
	}
	data, ok := solAccountDataMust[T](ctx, account, address)
	return account, data, ok
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

func solPreprocessIx(ctx *solContext, ix *db.SolanaInstruction) {
	switch ix.ProgramAddress {
	case SOL_SYSTEM_PROGRAM_ADDRESS:
		solPreprocessSystemIx(ctx, ix)
	case SOL_TOKEN_PROGRAM_ADDRESS:
		solPreprocessTokenIx(ctx, ix)
	case SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
		solPreprocessAssociatedTokenIx(ctx, ix)
	case SOL_SQUADS_V4_PROGRAM_ADDRESS:
		solPreprocessSquadsV4Ix(ctx, ix)
	case SOL_METEORA_FARMS_PROGRAM_ADDRESS:
		solPreprocessMeteoraFarmsIx(ctx, ix)
	case SOL_DRIFT_PROGRAM_ADDRESS:
		solPreprocessDriftIx(ctx, ix)
	}
}

func solPreprocessTx(
	ctx *solContext,
	ixs []*db.SolanaInstruction,
) {
	for ixIdx, ix := range ixs {
		ctx.ixIdx = uint32(ixIdx)
		solPreprocessIx(ctx, ix)
	}

	// NOTE: can only validate native balances, since we only keep track of those
nativeBalancesLoop:
	for address, nativeBalance := range ctx.nativeBalances {
		accountLifetimes, accountExists := ctx.accounts[address]
		tokens, isTokenAccount := ctx.tokenBalances[address]
		if isTokenAccount && ctx.walletOwned(tokens.Owner) {
			// NOTE: account has to exist if it is owned and it is opened
			// token account
			if !accountExists && nativeBalance.Post > 0 {
				err := solNewErrMissingAccount(ctx, address)
				ctx.errors = append(ctx.errors, err)
				continue
			}
		}

		// NOTE: accounts can be created/closed multiple times during single tx
		// so it's only possible to validate the balance of the last one that
		// is opened
		for i := len(accountLifetimes) - 1; i >= 0; i -= 1 {
			lt := accountLifetimes[i]
			if lt.PreparseOpen && lt.Owned {
				if nativeBalance.Post != lt.Balance {
					errData := db.ParserErrorAccountBalanceMismatch{
						Wallet:         address,
						AccountAddress: address,
						Token:          SOL_MINT_ADDRESS,
						External:       newDecimalFromRawAmount(nativeBalance.Post, 9),
						Local:          newDecimalFromRawAmount(lt.Balance, 9),
					}
					if isTokenAccount {
						errData.Wallet = tokens.Owner
					}
					err := &db.ParserError{
						TxId:  ctx.txId,
						IxIdx: int32(ctx.ixIdx),
						Type:  db.ParserErrorTypeAccountBalanceMismatch,
						Data:  &errData,
					}
					ctx.errors = append(ctx.errors, err)
				}

				if isTokenAccount {
					data, ok := solAccountDataMust[solTokenAccountData](ctx, &lt, address)
					if !ok {
						continue nativeBalancesLoop
					}

					found := false
					for _, t := range tokens.Tokens {
						if t.Token == data.Mint {
							found = true
							break
						}
					}
					if !found {
						err := &db.ParserError{
							TxId:  ctx.txId,
							IxIdx: int32(ctx.ixIdx),
							Type:  db.ParserErrorTypeAccountDataMismatch,
							Data: &db.ParserErrorAccountDataMismatch{
								AccountAddress: address,
								Message:        "Invalid token account mint",
							},
						}
						ctx.errors = append(ctx.errors, err)
						continue nativeBalancesLoop
					}
				}

				continue nativeBalancesLoop
			}
		}
	}
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
		account := ctx.findOwned(ctx.slot, ctx.ixIdx, address)
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
		solProcessTokenIx(ctx, ix, events)
	case SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
		solProcessAssociatedTokenIx(ctx, ix, events)
	case SOL_SQUADS_V4_PROGRAM_ADDRESS:
		solProcessSquadsV4Ix(ctx, ix, events)
	case SOL_METEORA_POOLS_PROGRAM_ADDRESS:
		solProcessMeteoraPoolsIx(ctx, ix, events)
	case SOL_JUP_V6_PROGRAM_ADDRESS:
		solProcessJupV6(ctx, ix, events)
	case SOL_METEORA_FARMS_PROGRAM_ADDRESS:
		solProcessMeteoraFarmsIx(ctx, ix, events)
	case SOL_MERCURIAL_PROGRAM_ADDRESS:
		solProcessMercurialIx(ctx, ix, events)
	case SOL_DRIFT_PROGRAM_ADDRESS:
		solProcessDriftIx(ctx, ix, events)
	}
}
