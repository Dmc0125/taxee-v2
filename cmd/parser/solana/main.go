package solana

import (
	"math"
	"taxee/pkg/db"
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

func errsEq(err1, err2 any) bool {
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

type ErrAccountMissing struct {
	TxId    string
	IxIdx   uint32
	Address string
}

func (err *ErrAccountMissing) init(ctx *Context, address string) {
	err.TxId = ctx.TxId
	err.IxIdx = ctx.IxIdx
	err.Address = address
}

func (err *ErrAccountMissing) IsEqual(other any) bool {
	return errsEq(err, other)
}

type ErrAccountBalanceMismatch struct {
	ErrAccountMissing
	Expected uint64
	Had      uint64
}

func (err *ErrAccountBalanceMismatch) init(
	ctx *Context,
	address string,
	expected, had uint64,
) {
	err.ErrAccountMissing.init(ctx, address)
	err.Expected = expected
	err.Had = had
}

func (err *ErrAccountBalanceMismatch) IsEqual(other any) bool {
	return errsEq(err, other)
}

type GlobalCtx interface {
	AppendErr(string, any)
	WalletOwned(db.Network, string) bool
}

type Context struct {
	GlobalCtx

	TxId           string
	Slot           uint64
	IxIdx          uint32
	TokensDecimals map[string]uint8
	Accounts       map[string][]AccountLifetime
}

func (ctx *Context) Init(wallets []string) {
	ctx.Accounts = make(map[string][]AccountLifetime)
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
		ctx.AppendErr(address, &err)
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
		ctx.AppendErr(closedAddress, &err)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		var err ErrAccountBalanceMismatch
		err.init(ctx, closedAddress, 0, 0)
		ctx.AppendErr(closedAddress, &err)
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

func (ctx *Context) accountFindOwned(address string, slot uint64, ixIdx uint32) *AccountLifetime {
	lifetimes, ok := ctx.Accounts[address]
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

func PreprocessTx(
	globalCtx GlobalCtx,
	accounts map[string][]AccountLifetime,
	txId string,
	tx *db.SolanaTransactionData,
) {
	ctx := Context{
		GlobalCtx: globalCtx,
		TxId:      txId,
		Slot:      tx.Slot,
		Accounts:  accounts,
	}

	for ixIdx, ix := range tx.Instructions {
		ctx.IxIdx = uint32(ixIdx)

		switch ix.ProgramAddress {
		case SYSTEM_PROGRAM_ADDRESS:
			preprocessSystemIx(&ctx, ix)
		case TOKEN_PROGRAM_ADDRESS:
			preprocessTokenIx(&ctx, ix)
		case ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
			preprocessAssociatedTokenIx(&ctx, ix)
		}
	}

	findLifetime := func(
		ctx *Context,
		lifetimes []AccountLifetime,
	) *AccountLifetime {
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
		if ctx.WalletOwned(db.NetworkSolana, accountAddress) {
			continue
		}

		lifetimes, accountExists := ctx.Accounts[accountAddress]
		tokenBalance, tokenBalanceExists := tx.TokenBalances[accountAddress]
		var lt *AccountLifetime

		switch {
		case tokenBalanceExists:
			if !ctx.WalletOwned(db.NetworkSolana, tokenBalance.Owner) {
				continue nativeBalances
			}

			if !accountExists {
				var err ErrAccountMissing
				err.init(&ctx, accountAddress)
				ctx.AppendErr(accountAddress, &err)
				continue nativeBalances
			}

			lt = findLifetime(&ctx, lifetimes)
			if lt == nil {
				var err ErrAccountMissing
				err.init(&ctx, accountAddress)
				ctx.AppendErr(accountAddress, &err)
				continue nativeBalances
			}
		case accountExists:
			lt = findLifetime(&ctx, lifetimes)
			if lt == nil || !lt.Owned {
				continue nativeBalances
			}
		default:
			continue nativeBalances
		}

		// TODO: maybe validate account data ?
		if lt.Balance != nativeBalance.Post {
			var err ErrAccountBalanceMismatch
			err.init(&ctx, accountAddress, nativeBalance.Post, lt.Balance)
			ctx.AppendErr(accountAddress, &err)
			continue nativeBalances
		}
	}
}

func ProcessIx(
	globalCtx GlobalCtx,
	accounts map[string][]AccountLifetime,
	events *[]db.Event,
	txId string,
	slot uint64,
	tokensDecimal map[string]uint8,
	ixIdx uint32,
	ix *db.SolanaInstruction,
) {
	ctx := Context{
		GlobalCtx:      globalCtx,
		Accounts:       accounts,
		TxId:           txId,
		Slot:           slot,
		TokensDecimals: tokensDecimal,
		IxIdx:          ixIdx,
	}

	switch ix.ProgramAddress {
	case SYSTEM_PROGRAM_ADDRESS:
		processSystemIx(&ctx, events, ix)
	}
}
