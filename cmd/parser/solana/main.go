package solana

import (
	"slices"
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
	Slot     uint64
	IxIdx    uint32
	Address  string
}

func (err *ErrAccountMissing) Error() string {
	return ""
}

func (err *ErrAccountMissing) init(ctx *Context, address string) {
	loc, e := location.Location(1)
	assert.NoErr(e, "")
	err.Location = loc
	err.Slot = ctx.Slot
	err.IxIdx = ctx.IxIdx
	err.Address = address
}

type ErrAccountBalanceMissing struct {
	ErrAccountMissing
	Expected uint64
	Had      uint64
}

func (err *ErrAccountBalanceMissing) Error() string {
	return ""
}

func (err *ErrAccountBalanceMissing) init(
	ctx *Context,
	address string,
	expected, had uint64,
) {
	loc, e := location.Location(1)
	assert.NoErr(e, "")
	err.Location = loc
	err.Slot = ctx.Slot
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
	case *ErrAccountBalanceMissing:
		e2, ok := err2.(*ErrAccountBalanceMissing)
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

func (ctx *Context) WalletOwned(other string) bool {
	return slices.Contains(ctx.Wallets, other)
}

func (ctx *Context) accountReceiveSol(address string, amount uint64) {
	lifetimes, ok := ctx.Accounts[address]

	if !ok {
		lifetimes = make([]AccountLifetime, 1)
	}
	if len(lifetimes) == 0 {
		lifetimes[0] = AccountLifetime{
			MinSlot:      ctx.Slot,
			MinIxIdx:     ctx.IxIdx,
			Balance:      amount,
			PreparseOpen: true,
		}
		ctx.Accounts[address] = lifetimes
		return
	}

	account := &lifetimes[len(lifetimes)-1]

	if account.PreparseOpen {
		account.Balance += amount
	} else {
		newAccount := AccountLifetime{
			MinSlot:      ctx.Slot,
			MinIxIdx:     ctx.IxIdx,
			Balance:      amount,
			PreparseOpen: true,
		}
		lifetimes = append(lifetimes, newAccount)
	}
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
		var err ErrAccountBalanceMissing
		err.init(ctx, address, 0, 0)
		return
	}

	acc.Data = data
	acc.Owned = true
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
		var err ErrAccountBalanceMissing
		err.init(ctx, closedAddress, 0, 0)
		ctx.appendErr(closedAddress, &err)
		return
	}

	acc.PreparseOpen = false
	acc.MaxSlot = ctx.Slot
	acc.MaxIxIdx = ctx.IxIdx

	ctx.accountReceiveSol(receiverAddress, acc.Balance)
}

func (ctx *Context) accountValidateNativeBalance(accountAddress string, expectedBalance uint64) {
	lifetimes, ok := ctx.Accounts[accountAddress]
	if !ok {
		var err ErrAccountMissing
		err.init(ctx, accountAddress)
		ctx.appendErr(accountAddress, &err)
		return
	}

	// NOTE: need to find the last account at current slot
	// - there could be multiple created and closed in one tx
	var lt *AccountLifetime
	for i := len(lifetimes) - 1; i >= 0; i -= 1 {
		temp := &lifetimes[i]

		if temp.MinSlot <= ctx.Slot && ctx.Slot <= temp.MaxSlot && temp.Owned {
			lt = temp
			break
		}
	}

	if lt == nil {
		var err ErrAccountMissing
		err.init(ctx, accountAddress)
		ctx.appendErr(accountAddress, &err)
		return
	}

	if lt.Balance != expectedBalance {
		var err ErrAccountBalanceMissing
		err.init(ctx, accountAddress, expectedBalance, lt.Balance)
		ctx.appendErr(accountAddress, &err)
	}
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

	// TODO: token balances stored are wrong
	// // NOTE: enforce token accounts are created and they have correct
	// // lamports balances
	// alreadyChecked := make([]string, 0)
	//
	// for accountAddress, tokenBalance := range tx.TokenBalances {
	// 	if !ctx.WalletOwned(tokenBalance.Owner) {
	// 		continue
	// 	}
	// 	if tokenBalance.Token != SOL_MINT_ADDRESS {
	// 		continue
	// 	}
	//
	// 	alreadyChecked = append(alreadyChecked, accountAddress)
	//
	// 	nativeBalance, ok := tx.NativeBalances[accountAddress]
	// 	assert.True(ok, "missing native balance for \"%s\" (txid=%s)", accountAddress, ctx.TxId)
	//
	// 	ctx.accountValidateNativeBalance(accountAddress, nativeBalance.Post)
	// }
	//
	// // NOTE: don't have a way of checking of accounts other than token accounts
	// // should exist so just compare lamports of created accounts with their
	// // expected lamports
	// for accountAddress, nativeBalance := range tx.NativeBalances {
	// 	if ctx.WalletOwned(accountAddress) {
	// 		continue
	// 	}
	//
	// 	tokenBalance, ok := tx.TokenBalances[accountAddress]
	// 	if !ok || ctx.WalletOwned(tokenBalance.Owner) {
	// 		continue
	// 	}
	//
	// 	ctx.accountValidateNativeBalance(accountAddress, nativeBalance.Post)
	// }
}
