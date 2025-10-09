package parser

import (
	"math"
	"slices"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"time"
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
		}
	}
}

type errAccountMissing struct {
	txId    string
	ixIdx   uint32
	address string
}

func newErrAccountMissing(ctx *solanaContext, address string) errAccountMissing {
	return errAccountMissing{
		txId:    ctx.txId,
		ixIdx:   ctx.ixIdx,
		address: address,
	}
}

func (err1 *errAccountMissing) IsEq(err2 parserError) bool {
	e2, ok := err2.(*errAccountMissing)
	if !ok {
		return false
	}
	return e2.address == err1.address
}

func (err *errAccountMissing) Address() string {
	return err.address
}

type errAccountBalanceMismatch struct {
	*errAccountMissing
	expected uint64
	had      uint64
}

func newErrAccountBalanceMismatch(
	ctx *solanaContext,
	address string,
	expected, had uint64,
) errAccountBalanceMismatch {
	e := newErrAccountMissing(ctx, address)
	return errAccountBalanceMismatch{
		errAccountMissing: &e,
		expected:          expected,
		had:               had,
	}
}

func (err1 *errAccountBalanceMismatch) IsEq(err2 parserError) bool {
	e2, ok := err2.(*errAccountBalanceMismatch)
	if !ok {
		return false
	}
	return err1.address == e2.address &&
		err1.expected == e2.expected &&
		err1.had == e2.had
}

func (err *errAccountBalanceMismatch) Address() string {
	return err.address
}

type accountLifetime struct {
	MinSlot  uint64
	MaxSlot  uint64
	MinIxIdx uint32
	MaxIxIdx uint32
	// used in preparse to know if an account is still
	// open inside one instruction
	PreparseOpen bool
	Balance      uint64
	Owned        bool
	Data         any
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
	parserErrors

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

func (ctx *solanaContext) initOwned(address string, data any) {
	lifetimes, ok := ctx.accounts[address]
	if !ok || len(lifetimes) == 0 {
		err := newErrAccountMissing(ctx, address)
		ctx.appendErr(&err)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		err := newErrAccountBalanceMismatch(ctx, address, 0, 0)
		ctx.appendErr(&err)
		return
	}

	acc.Data = data
	acc.Owned = true
	ctx.accounts[address] = lifetimes
}

func (ctx *solanaContext) close(closedAddress, receiverAddress string) {
	lifetimes, ok := ctx.accounts[closedAddress]
	if !ok || len(lifetimes) == 0 {
		err := newErrAccountMissing(ctx, closedAddress)
		ctx.appendErr(&err)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		err := newErrAccountBalanceMismatch(ctx, closedAddress, 0, 0)
		ctx.appendErr(&err)
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

func (ctx *solanaContext) findOwned(
	slot uint64,
	ixIdx uint32,
	address string,
) *accountLifetime {
	lifetimes, ok := ctx.accounts[address]
	if !ok || len(lifetimes) == 0 {
		return nil
	}

	for i := 0; i < len(lifetimes); i += 1 {
		lt := &lifetimes[i]
		if lt.open(slot, ixIdx) && lt.Owned {
			return lt
		}
	}

	return nil
}

func (ctx *solanaContext) initEvent(event *db.Event) {
	event.Network = db.NetworkSolana
	event.TxId = ctx.txId
	event.IxIdx = int32(ctx.ixIdx)
	event.Timestamp = ctx.timestamp
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

func solPreprocessTx(
	errors parserErrors,
	solanaAccounts map[string][]accountLifetime,
	wallets []string,
	txId string,
	txData *db.SolanaTransactionData,
) {
	ctx := solanaContext{
		wallets:      wallets,
		accounts:     solanaAccounts,
		parserErrors: errors,

		txId:           txId,
		slot:           txData.Slot,
		tokensDecimals: txData.TokenDecimals,
	}

	for ixIdx, ix := range txData.Instructions {
		ctx.ixIdx = uint32(ixIdx)

		switch ix.ProgramAddress {
		case SOL_SYSTEM_PROGRAM_ADDRESS:
			solPreprocessSystemIx(&ctx, ix)
		case SOL_TOKEN_PROGRAM_ADDRESS:
			solPreprocessTokenIx(&ctx, ix)
		case SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
			solPreprocessAssociatedTokenIx(&ctx, ix)
		}
	}
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
	}
}
