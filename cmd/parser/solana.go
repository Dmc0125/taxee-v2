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

type solanaContext struct {
	wallets  []string
	accounts map[string][]accountLifetime

	preprocessErrors map[string][]*db.ParserError
	processErrors    map[string][]*db.ParserError

	// volatile for each tx/ix
	txId           string
	slot           uint64
	timestamp      time.Time
	ixIdx          uint32
	tokensDecimals map[string]uint8
}

func (ctx *solanaContext) walletOwned(other string) bool {
	return slices.Contains(ctx.wallets, other)
}

func (ctx *solanaContext) receiveSol(address string, amount uint64) {
	lifetimes, ok := ctx.accounts[address]

	if !ok {
		lifetimes = make([]accountLifetime, 0)
	}
	if len(lifetimes) == 0 {
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

func solNewErrMissingAccount(ctx *solanaContext, address string) *db.ParserError {
	e := db.ParserError{
		TxId:  ctx.txId,
		IxIdx: ctx.ixIdx,
		Type:  db.ParserErrorTypeMissingAccount,
		Data: &db.ParserErrorMissingAccount{
			AccountAddress: address,
		},
	}
	return &e
}

func solNewErrAccountBalanceMismatch(
	ctx *solanaContext,
	address, token string,
	expected, had decimal.Decimal,
) *db.ParserError {
	d := db.ParserErrorAccountBalanceMismatch{
		AccountAddress: address,
		Token:          token,
		Expected:       expected,
		Had:            had,
	}

	return &db.ParserError{
		TxId:  ctx.txId,
		IxIdx: ctx.ixIdx,
		Type:  db.ParserErrorTypeAccountBalanceMismatch,
		Data:  d,
	}
}

func (ctx *solanaContext) init(address string, owned bool, data any) {
	lifetimes, ok := ctx.accounts[address]
	if !ok || len(lifetimes) == 0 {
		err := solNewErrMissingAccount(ctx, address)
		appendErrUnique(ctx.preprocessErrors, err)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		err := solNewErrMissingAccount(ctx, address)
		appendErrUnique(ctx.preprocessErrors, err)
		return
	}

	acc.Data = data
	acc.Owned = owned
	ctx.accounts[address] = lifetimes
}

func (ctx *solanaContext) close(closedAddress, receiverAddress string) {
	lifetimes, ok := ctx.accounts[closedAddress]
	if !ok || len(lifetimes) == 0 {
		err := solNewErrMissingAccount(ctx, closedAddress)
		appendErrUnique(ctx.preprocessErrors, err)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		err := solNewErrMissingAccount(ctx, closedAddress)
		appendErrUnique(ctx.preprocessErrors, err)
		return
	}

	closedBalance := acc.Balance

	acc.PreparseOpen = false
	acc.MaxSlot = ctx.slot
	acc.MaxIxIdx = ctx.ixIdx
	acc.Balance = 0
	ctx.accounts[closedAddress] = lifetimes

	ctx.receiveSol(receiverAddress, closedBalance)
}

// 0 -> not owned, 1 -> owned, 2 -> whatever
func (ctx *solanaContext) find(
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

func (ctx *solanaContext) findOwned(
	slot uint64,
	ixIdx uint32,
	address string,
) *accountLifetime {
	return ctx.find(slot, ixIdx, address, 1)
}

func solNewEvent(ctx *solanaContext) *db.Event {
	return &db.Event{
		Timestamp: ctx.timestamp,
		Network:   db.NetworkSolana,
		TxId:      ctx.txId,
		IxIdx:     int32(ctx.ixIdx),
	}
}

func solAccountDataMust[T any](account *accountLifetime) *T {
	data, ok := account.Data.(*T)
	assert.True(ok, "invalid account data: %T", account.Data)
	return data
}

func solDecimalsMust(ctx *solanaContext, mint string) uint8 {
	decimals, ok := ctx.tokensDecimals[mint]
	assert.True(ok, "missing decimals for: %s", mint)
	return decimals
}

func solPreprocessIx(ctx *solanaContext, ix *db.SolanaInstruction) {
	switch ix.ProgramAddress {
	case SOL_SYSTEM_PROGRAM_ADDRESS:
		solPreprocessSystemIx(ctx, ix)
	case SOL_TOKEN_PROGRAM_ADDRESS:
		solPreprocessTokenIx(ctx, ix)
	case SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
		solPreprocessAssociatedTokenIx(ctx, ix)
	case SOL_SQUADS_V4_PROGRAM_ADDRESS:
		solPreprocessSquadsV4Ix(ctx, ix)
	}
}

func solPreprocessTx(
	ctx *solanaContext,
	ixs []*db.SolanaInstruction,
) {
	for ixIdx, ix := range ixs {
		ctx.ixIdx = uint32(ixIdx)
		solPreprocessIx(ctx, ix)
	}

	// TODO: Validate balances
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
	ctx *solanaContext,
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
		fromInternal, toInternal,
		newDecimalFromRawAmount(amount, 9),
		SOL_MINT_ADDRESS,
		uint16(db.NetworkSolana),
	)

	return event, true, nil
}

func solProcessIx(
	events *[]*db.Event,
	ctx *solanaContext,
	ix *db.SolanaInstruction,
) {
	switch ix.ProgramAddress {
	case SOL_SYSTEM_PROGRAM_ADDRESS:
		solProcessSystemIx(ctx, ix, events)
	case SOL_TOKEN_PROGRAM_ADDRESS:
		solProcessTokenIx(ctx, ix, events)
	case SOL_SQUADS_V4_PROGRAM_ADDRESS:
		solProcessSquadsV4Ix(ctx, ix, events)
	}
}
