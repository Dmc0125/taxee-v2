package parser

import (
	"math"
	"slices"
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
		case SOL_SQUADS_V4_PROGRAM_ADDRESS:
			solSquadsV4IxRelatedAccounts(relatedAccounts, walletAddress, ix)
		case SOL_JUP_V6_PROGRAM_ADDRESS:
			solJupV6RelatedAccounts(relatedAccounts, walletAddress, ix)
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
	if acc.MinSlot == slot && acc.MinIxIdx <= ixIdx {
		return true
	}
	if acc.MaxSlot == slot && acc.MaxIxIdx >= ixIdx {
		return true
	}
	return acc.MinSlot < slot && slot < acc.MaxSlot
}

type solContext struct {
	wallets   []string
	accounts  map[string][]accountLifetime
	errors    *[]*db.ParserError
	errOrigin db.ErrOrigin

	events *[]*db.Event

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

func solNewErrMissingAccount(ctx *solContext, address string) {
	err := solNewError(ctx)
	err.Type = db.ParserErrorTypeMissingAccount
	err.Data = &db.ParserErrorMissingAccount{
		AccountAddress: address,
	}
}

func (ctx *solContext) init(address string, owned bool, data any) {
	lifetimes, ok := ctx.accounts[address]
	if !ok || len(lifetimes) == 0 {
		solNewErrMissingAccount(ctx, address)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		solNewErrMissingAccount(ctx, address)
		return
	}

	acc.Data = data
	acc.Owned = owned
	ctx.accounts[address] = lifetimes
}

func (ctx *solContext) close(closedAddress string) {
	lifetimes, ok := ctx.accounts[closedAddress]
	if !ok || len(lifetimes) == 0 {
		solNewErrMissingAccount(ctx, closedAddress)
		return
	}

	acc := &lifetimes[len(lifetimes)-1]
	if !acc.PreparseOpen {
		solNewErrMissingAccount(ctx, closedAddress)
		return
	}

	acc.PreparseOpen = false
	acc.MaxSlot = ctx.slot
	acc.MaxIxIdx = ctx.ixIdx
	ctx.accounts[closedAddress] = lifetimes
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
		solNewErrMissingAccount(ctx, address)
	} else {
		ok = true
	}
	return
}

func solPreprocessIx(ctx *solContext, ix *db.SolanaInstruction) {
	switch ix.ProgramAddress {
	case SOL_SYSTEM_PROGRAM_ADDRESS:
		solPreprocessSystemIx(ctx, ix)
	case SOL_TOKEN_PROGRAM_ADDRESS,
		// TODO: probably will need special handling
		SOL_TOKEN2022_PROGRAM_ADDRESS:
		solPreprocessTokenIx(ctx, ix)
	case SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
		solPreprocessAssociatedTokenIx(ctx, ix)
	case SOL_SQUADS_V4_PROGRAM_ADDRESS:
		solPreprocessSquadsV4Ix(ctx, ix)
	case SOL_METEORA_FARMS_PROGRAM_ADDRESS:
		solPreprocessMeteoraFarmsIx(ctx, ix)
	case SOL_DRIFT_PROGRAM_ADDRESS:
		solPreprocessDriftIx(ctx, ix)
	case SOL_JUP_DCA_PROGRAM_ADDRESS:
		solPreprocessJupDcaIx(ctx, ix)
	case SOL_ALT_PROGRAM_ADDRESS:
		solPreprocessAltIx(ctx, ix)
	case SOL_KAMINO_LEND_PROGRAM_ADDRESS:
		solPreprocessKaminoLendIx(ctx, ix)
	case SOL_MANGO_V4_PROGRAM_ADDRESS, SOL_MANGO_V4_PROGRAM_ADDRESS_2:
		solPreprocessMangoV4Ix(ctx, ix)
	case SOL_CIRCUIT_PROGRAM_ADDRESS:
		solPreprocessCircuitIx(ctx, ix)
	case SOL_JUP_V6_PROGRAM_ADDRESS:
		solPreprocessJupV6(ctx, ix)
	case SOL_SYNATRA_PROGRAM_ADDRESS:
		solPreprocessSynatraIx(ctx, ix)
	case SOL_LULO_PROGRAM_ADDRESS:
		solPreprocessLuloIx(ctx, ix)
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
				solNewErrMissingAccount(ctx, address)
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
					err := solNewError(ctx)
					err.Type = db.ParserErrorTypeAccountBalanceMismatch
					err.Data = &errData
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
						err := solNewError(ctx)
						err.Type = db.ParserErrorTypeAccountDataMismatch
						err.Data = &db.ParserErrorAccountDataMismatch{
							AccountAddress: address,
							Message:        "Invalid token account mint",
						}
						continue nativeBalancesLoop
					}
				}

				continue nativeBalancesLoop
			}
		}
	}
}

func solProcessIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
) {
	switch ix.ProgramAddress {
	case SOL_SYSTEM_PROGRAM_ADDRESS:
		solProcessSystemIx(ctx, ix)
	case SOL_TOKEN_PROGRAM_ADDRESS,
		SOL_TOKEN2022_PROGRAM_ADDRESS:
		solProcessTokenIx(ctx, ix)
	case SOL_ASSOCIATED_TOKEN_PROGRAM_ADDRESS:
		solProcessAssociatedTokenIx(ctx, ix)
	case SOL_SQUADS_V4_PROGRAM_ADDRESS:
		solProcessSquadsV4Ix(ctx, ix)
	case SOL_METEORA_POOLS_PROGRAM_ADDRESS:
		solProcessMeteoraPoolsIx(ctx, ix)
	case SOL_JUP_V6_PROGRAM_ADDRESS:
		solProcessJupV6(ctx, ix)
	case SOL_METEORA_FARMS_PROGRAM_ADDRESS:
		solProcessMeteoraFarmsIx(ctx, ix)
	case SOL_MERCURIAL_PROGRAM_ADDRESS:
		solProcessMercurialIx(ctx, ix)
	case SOL_DRIFT_PROGRAM_ADDRESS:
		solProcessDriftIx(ctx, ix)
	case SOL_JUP_DCA_PROGRAM_ADDRESS:
		solProcessJupDcaIx(ctx, ix)
	case SOL_ALT_PROGRAM_ADDRESS:
		solProcessAltIx(ctx, ix)
	case SOL_KAMINO_LEND_PROGRAM_ADDRESS:
		solProcessKaminoLendIx(ctx, ix)
	case SOL_MANGO_V4_PROGRAM_ADDRESS, SOL_MANGO_V4_PROGRAM_ADDRESS_2:
		solProcessMangoV4Ix(ctx, ix)
	case SOL_CIRCUIT_PROGRAM_ADDRESS:
		solProcessCircuitIx(ctx, ix)
	case SOL_SANCTUM_PROGRAM_ADDRESS:
		solProcessSanctumIx(ctx, ix)
	case SOL_DRIFT_DISTRIBUTOR:
		solProcessMerkleDistributorIx(ctx, ix, "drift")
	case SOL_SYNATRA_PROGRAM_ADDRESS:
		solProcessSynatraIx(ctx, ix)
	case SOL_CARROT_PROGRAM_ADDRESS:
		solProcessCarrotIx(ctx, ix)
	case SOL_LULO_PROGRAM_ADDRESS:
		solProcessLuloIx(ctx, ix)
	case SOL_DFLOW_PROGRAM_ADDRESS:
		solProcessDflowIx(ctx, ix)
	}
}
