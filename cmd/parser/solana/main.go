package solana

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/location"
)

type AccountLifetime struct {
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

func (acc *AccountLifetime) initUninitialized(ctx *Context) {
	acc.MinSlot = ctx.Slot
	acc.MinIxIdx = ctx.IxIdx
	acc.MaxSlot = math.MaxUint64
	acc.MaxIxIdx = math.MaxUint32
	acc.PreparseOpen = true
}

func (acc *AccountLifetime) open(slot uint64, ixIdx uint32) bool {
	if acc.MinSlot == slot && acc.MinIxIdx < ixIdx {
		return true
	}
	if acc.MaxSlot == slot && acc.MaxIxIdx > ixIdx {
		return true
	}
	return acc.MinSlot < slot && slot < acc.MaxSlot
}

type ErrAccountMissing struct {
	Location string
	TxId     string
	IxIdx    uint32
	Address  string
}

func (err *ErrAccountMissing) Error() string {
	m := fmt.Sprintf(
		"(%s)\nAccount \"%s\" missing at %s (ix idx: %d)",
		err.Location, err.Address, err.TxId, err.IxIdx,
	)
	return m
}

func (err *ErrAccountMissing) init(ctx *Context, address string) {
	loc, e := location.Location(2)
	assert.NoErr(e, "")
	err.Location = loc
	err.TxId = ctx.TxId
	err.IxIdx = ctx.IxIdx
	err.Address = address
}

type ErrAccountBalanceMismatch struct {
	ErrAccountMissing
	Expected uint64
	Had      uint64
}

func (err *ErrAccountBalanceMismatch) Error() string {
	m := fmt.Sprintf(
		"(%s)\nAccount \"%s\" balance missing at %s (ix idx: %d) - expected: %d != had: %d",
		err.Location, err.Address, err.TxId, err.IxIdx, err.Expected, err.Had,
	)
	return m
}

func (err *ErrAccountBalanceMismatch) init(
	ctx *Context,
	address string,
	expected, had uint64,
) {
	loc, e := location.Location(2)
	assert.NoErr(e, "")
	err.Location = loc
	err.TxId = ctx.TxId
	err.IxIdx = ctx.IxIdx
	err.Address = address
	err.Expected = expected
	err.Had = had
}

func errEq(err1, err2 error) bool {
	switch e1 := err1.(type) {
	case *ErrAccountMissing:
		e2, ok := err2.(*ErrAccountMissing)
		if !ok {
			return false
		}
		return e1.Address == e2.Address
	case *ErrAccountBalanceMismatch:
		e2, ok := err2.(*ErrAccountBalanceMismatch)
		if !ok {
			return false
		}
		return e1.Address == e2.Address && e1.Expected == e2.Expected && e1.Had == e2.Had
	}

	return false
}

type Context struct {
	TxId  string
	Slot  uint64
	IxIdx uint32

	Wallets  []string
	Accounts map[string][]AccountLifetime
	Errors   map[string][]error
}

func (ctx *Context) Init(wallets []string) {
	ctx.Accounts = make(map[string][]AccountLifetime)
	ctx.Errors = make(map[string][]error)
	ctx.Wallets = wallets
}

func (ctx *Context) appendErr(address string, err error) {
	errors, ok := ctx.Errors[address]

	switch {
	case !ok:
		errors = make([]error, 0)
		fallthrough
	case len(errors) == 0:
		errors = append(errors, err)
	default:
		last := errors[len(errors)-1]
		if errEq(err, last) {
			return
		}
		errors = append(errors, err)
	}

	ctx.Errors[address] = errors
}

func (ctx *Context) walletOwned(other string) bool {
	return slices.Contains(ctx.Wallets, other)
}

func (ctx *Context) accountReceiveSol(address string, amount uint64) {
	lifetimes, ok := ctx.Accounts[address]

	if !ok {
		lifetimes = make([]AccountLifetime, 0)
	}
	if len(lifetimes) == 0 {
		var acc AccountLifetime
		acc.initUninitialized(ctx)
		acc.Balance = amount
		lifetimes = append(lifetimes, acc)
		ctx.Accounts[address] = lifetimes
		return
	}

	account := &lifetimes[len(lifetimes)-1]

	if account.PreparseOpen {
		account.Balance += amount
	} else {
		var acc AccountLifetime
		acc.initUninitialized(ctx)
		acc.Balance = amount
		lifetimes = append(lifetimes, acc)
	}

	ctx.Accounts[address] = lifetimes
}

func (ctx *Context) accountInitOwned(address string, data any) {
	lifetimes, ok := ctx.Accounts[address]
	if !ok || len(lifetimes) == 0 {
		var err ErrAccountMissing
		err.init(ctx, address)
		ctx.appendErr(address, &err)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		var err ErrAccountBalanceMismatch
		err.init(ctx, address, 0, 0)
		return
	}

	acc.Data = data
	acc.Owned = true
	ctx.Accounts[address] = lifetimes
}

func (ctx *Context) accountClose(closedAddress, receiverAddress string) {
	lifetimes, ok := ctx.Accounts[closedAddress]
	if !ok || len(lifetimes) == 0 {
		var err ErrAccountMissing
		err.init(ctx, closedAddress)
		ctx.appendErr(closedAddress, &err)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		var err ErrAccountBalanceMismatch
		err.init(ctx, closedAddress, 0, 0)
		ctx.appendErr(closedAddress, &err)
		return
	}

	closedBalance := acc.Balance

	acc.PreparseOpen = false
	acc.MaxSlot = ctx.Slot
	acc.MaxIxIdx = ctx.IxIdx
	acc.Balance = 0
	ctx.Accounts[closedAddress] = lifetimes

	ctx.accountReceiveSol(receiverAddress, closedBalance)
}

func (ctx *Context) AccountsString() string {
	sb := strings.Builder{}

	pad := func(x string, args ...any) string {
		m := fmt.Sprintf(x, args...)
		return fmt.Sprintf("    %s", m)
	}

	for address, accounts := range ctx.Accounts {
		sb.WriteRune('\n')
		sb.WriteString("Account: ")
		sb.WriteString(address)
		sb.WriteRune('\n')

		for i, acc := range accounts {
			// sb.WriteRune('\n')
			sb.WriteRune(rune(48 + i))
			sb.WriteRune(':')
			sb.WriteRune('\n')

			sb.WriteString(pad(fmt.Sprintf(
				"Min: slot %d (ix idx: %d) <> Max: slot %d (ix idx: %d)\n",
				acc.MinSlot,
				acc.MinIxIdx,
				acc.MaxSlot,
				acc.MaxIxIdx,
			)))
			sb.WriteString(pad("Balance: %d\n", acc.Balance))
			sb.WriteString(pad("Data: %#v\n", acc.Data))
			sb.WriteString(pad("Owned: %t\n", acc.Owned))
		}
	}

	return sb.String()
}

func (ctx *Context) ErrorsString() string {
	sb := strings.Builder{}

	for address, errs := range ctx.Errors {
		sb.WriteString("\nAccount errors: ")
		sb.WriteString(address)
		sb.WriteRune('\n')

		for i, err := range errs {
			sb.WriteRune(rune(48 + i))
			sb.WriteRune(':')
			sb.WriteString(err.Error())
			sb.WriteRune('\n')
		}
	}

	return sb.String()
}

type relatedAccounts interface {
	Append(string)
}

func RelatedAccountsFromTx(
	accounts relatedAccounts,
	walletAddress string,
	tx *db.SolanaTransactionData,
) {
	for _, ix := range tx.Instructions {
		switch ix.ProgramAddress {
		case TOKEN_PROGRAM_ADDRESS:
			tokenIxRelatedAccounts(accounts, walletAddress, ix)
		case ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
			associatedTokenIxRelatedAccounts(accounts, walletAddress, ix)
		}
	}
}

func PreparseTx(ctx *Context, tx *db.SolanaTransactionData) {
	ctx.Slot = tx.Slot

	for ixIdx, ix := range tx.Instructions {
		ctx.IxIdx = uint32(ixIdx)

		switch ix.ProgramAddress {
		case SYSTEM_PROGRAM_ADDRESS:
			preprocessSystemIx(ctx, ix)
		case TOKEN_PROGRAM_ADDRESS:
			preprocessTokenIx(ctx, ix)
		case ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
			preprocessAssociatedTokenIx(ctx, ix)
		}
	}

	findLifetime := func(ctx *Context, lifetimes []AccountLifetime) *AccountLifetime {
		var lt *AccountLifetime
		for i := len(lifetimes) - 1; i >= 0; i -= 1 {
			temp := &lifetimes[i]
			if temp.MinSlot <= ctx.Slot && ctx.Slot <= temp.MaxSlot {
				lt = temp
				break
			}
		}
		return lt
	}

nativeBalances:
	for accountAddress, nativeBalance := range tx.NativeBalances {
		if ctx.walletOwned(accountAddress) {
			continue
		}

		lifetimes, accountExists := ctx.Accounts[accountAddress]
		tokenBalance, tokenBalanceExists := tx.TokenBalances[accountAddress]
		var lt *AccountLifetime

		switch {
		case tokenBalanceExists:
			if !ctx.walletOwned(tokenBalance.Owner) {
				continue nativeBalances
			}

			if !accountExists {
				var err ErrAccountMissing
				err.init(ctx, accountAddress)
				ctx.appendErr(accountAddress, &err)
				continue nativeBalances
			}

			lt = findLifetime(ctx, lifetimes)
			if lt == nil {
				var err ErrAccountMissing
				err.init(ctx, accountAddress)
				ctx.appendErr(accountAddress, &err)
				continue nativeBalances
			}
		case accountExists:
			lt = findLifetime(ctx, lifetimes)
			if lt == nil || !lt.Owned {
				continue nativeBalances
			}
		default:
			continue nativeBalances
		}

		// TODO: maybe validate account data ?
		if lt.Balance != nativeBalance.Post {
			var err ErrAccountBalanceMismatch
			err.init(ctx, accountAddress, nativeBalance.Post, lt.Balance)
			ctx.appendErr(accountAddress, &err)
			continue nativeBalances
		}
	}
}
