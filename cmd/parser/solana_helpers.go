package parser

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/db"

	"github.com/shopspring/decimal"
)

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
		err := solNewError(ctx)
		err.Type = db.ParserErrorTypeAccountDataMismatch
		err.Data = &db.ParserErrorAccountDataMismatch{
			AccountAddress: address,
			Message:        fmt.Sprintf("Expected the data to be %s", temp.name()),
		}
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

func solDecimals(ctx *solContext, mint string) (uint8, bool) {
	// NOTE: this should **always** be ok, if it is not, RPC api changed and
	// we want to know that
	decimals, ok := ctx.tokensDecimals[mint]
	if !ok {
		slog.Error("missing decimals", "mint", mint, "txId", ctx.txId)
	}
	return decimals, ok
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

func (iter *solInnerIxIterator) peekNext() (*db.SolanaInnerInstruction, bool) {
	if !iter.hasNext() {
		return nil, false
	}
	return iter.innerIxs[iter.pos], true
}

func (iter *solInnerIxIterator) next() *db.SolanaInnerInstruction {
	ix := iter.innerIxs[iter.pos]
	iter.pos += 1
	return ix
}

func (iter *solInnerIxIterator) nextSafe() (*db.SolanaInnerInstruction, bool) {
	if iter.hasNext() {
		return iter.next(), true
	}
	return nil, false
}

func solParseCreateAssociatedTokenAccount(
	accounts []string,
	innerIxs *solInnerIxIterator,
) (payer, owner, tokenAccount, mint string, amount uint64) {
	if _, ok := innerIxs.nextSafe(); !ok {
		return
	}

	// ixs:
	// 1. get account len
	//
	// if new account is empty
	// 	2. create account
	// if has balance but not enough
	// 	2. transfer
	//  3. allocate
	// 	4. assign
	// if has enough balance
	// 	2. allocate
	// 	3. assign
	//
	// .. init immutable owner
	// .. init account

	// TODO: should we even check?? createAta is built in ix, which will most
	// likely never change - probably should be asserted as it would prevent
	// us from parsing anything that is not create ata ix

	// skip getAccountDataSize
	transferIx, ok := innerIxs.nextSafe()
	if !ok {
		// err
		return
	}

	switch transferIx.ProgramAddress {
	case SOL_SYSTEM_PROGRAM_ADDRESS:
		transfer, ok := solParseSystemIxSolTransfer(transferIx.Accounts, transferIx.Data)
		if !ok {
			return
		}
		payer, tokenAccount, amount = transfer.from, transfer.to, transfer.amount

		switch transfer.ix {
		case solSystemIxCreate:
		case solSystemIxTransfer:
			// skip allocate and assign
			_, ok1 := innerIxs.nextSafe()
			_, ok2 := innerIxs.nextSafe()
			if !ok1 || !ok2 {
				// error
				return
			}
		default:
			// error
			return
		}

	default:
		// skip allocate and assign
		_, ok1 := innerIxs.nextSafe()
		_, ok2 := innerIxs.nextSafe()
		if !ok1 || !ok2 {
			// error
			return
		}

		payer, tokenAccount = accounts[0], accounts[1]
	}

	// skip init immutable owner and init account
	_, ok1 := innerIxs.nextSafe()
	_, ok2 := innerIxs.nextSafe()
	if !ok1 || !ok2 {
		// error
		return
	}

	owner, mint = accounts[2], accounts[3]
	return
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

	transfer, ok := solParseSystemIxSolTransfer(firstIx.Accounts, firstIx.Data)
	if !ok {
		err = fmt.Errorf("invalid anchor init ix: %#v", *firstIx)
		return
	}
	from, to, amount = transfer.from, transfer.to, transfer.amount

	switch transfer.ix {
	case solSystemIxCreate:
	case solSystemIxTransfer:
		// NOTE: skip over allocate and assign
		_, ok1 := innerIxs.nextSafe()
		_, ok2 := innerIxs.nextSafe()
		if !ok1 || !ok2 {
			err = errors.New("invalid anchor init ix")
			return
		}
	default:
		assert.True(false, "invalid anchor init ix: %d", transfer.ix)
	}

	return
}

func solPreprocessAnchorInitAccount(
	ctx *solContext,
	owner string,
	innerIxs []*db.SolanaInnerInstruction,
) error {
	if !slices.Contains(ctx.wallets, owner) {
		return nil
	}

	_, to, amount, err := solAnchorInitAccountValidate(&solInnerIxIterator{
		innerIxs: innerIxs,
	})
	if err != nil {
		return err
	}

	ctx.receiveSol(to, amount)
	ctx.init(to, true, nil)
	return nil
}

// NOTE: implemented based on
// https://github.com/solana-foundation/anchor/blob/master/lang/syn/src/codegen/accounts/constraints.rs#L1036
func solProcessAnchorInitAccount(
	ctx *solContext,
	innerIxs *solInnerIxIterator,
	owner, app, method string,
) (bool, error) {
	from, to, amount, err := solAnchorInitAccountValidate(innerIxs)
	if err != nil {
		return false, err
	}

	// NOTE: can only happen if unknown account is being created (squads tx)
	fromInternal := ctx.walletOwned(from)
	toInternal := slices.Contains(ctx.wallets, to) || ctx.findOwned(ctx.slot, ctx.ixIdx, to) != nil

	if !fromInternal && !toInternal {
		return false, nil
	}

	direction := getTransferEventDirection(fromInternal, toInternal)

	toWallet := owner
	if !toInternal {
		toWallet = to
	}

	event := solNewEvent(ctx, app, method, db.EventTypeTransfer)
	event.Transfers = append(event.Transfers, &db.EventTransfer{
		Direction:  direction,
		FromWallet: from,
		// TODO: not correct
		ToWallet:    toWallet,
		FromAccount: from,
		ToAccount:   to,
		Token:       SOL_MINT_ADDRESS,
		Amount:      newDecimalFromRawAmount(amount, 9),
		TokenSource: uint16(db.NetworkSolana),
	})

	return true, nil
}

func solParseTransfers(
	ctx *solContext,
	innerIxs []*db.SolanaInnerInstruction,
) ([]*db.EventTransfer, int, int) {
	amounts := make(map[string]decimal.Decimal)
	tokenAccounts := make(map[string]string)
	var wallet string

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
		case SOL_TOKEN_PROGRAM_ADDRESS, SOL_TOKEN2022_PROGRAM_ADDRESS:
			t, ok := solParseTokenIxTokenTransfer(innerIx.Accounts, innerIx.Data)
			if !ok || !t.ix.economic() {
				continue
			}

			if t.from != "" {
				if validMint, w, ok := getTokenAccount(t.from, t.mint); ok {
					wallet = w
					decimals, ok := solDecimals(ctx, validMint)
					if !ok {
						// TODO: return ?
						continue
					}
					amounts[validMint] = amounts[validMint].Sub(
						newDecimalFromRawAmount(t.amount, decimals),
					)
					tokenAccounts[validMint] = t.from
				}
			}
			if t.to != "" {
				if validMint, w, ok := getTokenAccount(t.to, t.mint); ok {
					wallet = w
					decimals, ok := solDecimals(ctx, validMint)
					if !ok {
						// TODO: return ?
						continue
					}
					amounts[validMint] = amounts[validMint].Add(
						newDecimalFromRawAmount(t.amount, decimals),
					)
					tokenAccounts[validMint] = t.to
				}
			}
		}
	}

	transfers := make([]*db.EventTransfer, 0)
	var outgoingCount, incomingCount int

	for mint, amount := range amounts {
		if amount.Equal(decimal.Zero) {
			continue
		}

		tokenAccount, ok := tokenAccounts[mint]
		assert.True(ok, "missing token account for mint")

		t := db.EventTransfer{
			Token:       mint,
			Amount:      amount.Abs(),
			TokenSource: uint16(db.NetworkSolana),
		}

		if amount.IsNegative() {
			outgoingCount += 1
			t.Direction = db.EventTransferOutgoing
			t.FromAccount = tokenAccount
			t.FromWallet = wallet
			transfers = append(transfers, &t)
		} else {
			incomingCount += 1
			t.Direction = db.EventTransferIncoming
			t.ToAccount = tokenAccount
			t.ToWallet = wallet
			transfers = append(transfers, &t)
		}
	}

	slices.SortFunc(transfers, func(a, b *db.EventTransfer) int {
		adir, bdir := a.Direction, b.Direction
		if adir == bdir {
			return strings.Compare(a.Token, b.Token)
		}
		return adir.Cmp(bdir)
	})

	return transfers, outgoingCount, incomingCount
}

func solSwapEventFromTransfers(
	ctx *solContext,
	innerInstructions []*db.SolanaInnerInstruction,
	app, method string,
	eventType db.EventType,
) {
	transfers, outgoingCount, incomingCount := solParseTransfers(ctx, innerInstructions)
	if outgoingCount == 0 || incomingCount == 0 {
		err := solNewError(ctx)
		err.Type = db.ParserErrorTypeOneSidedSwap
		return
	}

	event := solNewEvent(ctx, app, method, eventType)
	event.Transfers = transfers
}

// deposit => 0
// withdraw => 1
func solNewLendingStakeEvent(
	ctx *solContext,
	owner, userAddress string,
	transferIx *db.SolanaInnerInstruction,
	direction int,
	app, method string,
) {
	if !slices.Contains(ctx.wallets, owner) {
		return
	}

	if _, ok := ctx.findOwnedOrError(ctx.slot, ctx.ixIdx, userAddress); !ok {
		return
	}

	t, ok := solParseTokenIxTokenTransfer(transferIx.Accounts, transferIx.Data)
	if !ok || (t.ix != solTokenIxTransfer && t.ix != solTokenIxTransferChecked) {
		return
	}

	fromAccount, toAccount := t.from, t.to
	var tokenAccountAddress string
	// NOTE: needs to be done because lending/staking is adding to the balance
	// which we can not really track, if it were not stored on a separate account
	// when withdrawal with interest happens, the "real" SOL balance needed for
	// creating the account would be subtracted too, leaving and empty program
	// account, even though it's not really empty
	userAddress = fmt.Sprintf("%s:program_account", userAddress)

	switch direction {
	// deposit
	case 0:
		tokenAccountAddress = fromAccount
		toAccount = userAddress
	// withdraw
	case 1:
		tokenAccountAddress = toAccount
		fromAccount = userAddress
	}

	_, tokenAccount, ok := solAccountExactOrError[solTokenAccountData](
		ctx, tokenAccountAddress,
	)
	if !ok {
		return
	}

	decimals, ok := solDecimals(ctx, tokenAccount.Mint)
	if !ok {
		return
	}

	event := solNewEvent(ctx, app, method, db.EventTypeTransfer)
	event.Transfers = append(event.Transfers, &db.EventTransfer{
		Direction:   db.EventTransferInternal,
		FromWallet:  owner,
		FromAccount: fromAccount,
		ToWallet:    owner,
		ToAccount:   toAccount,
		Token:       tokenAccount.Mint,
		Amount:      newDecimalFromRawAmount(t.amount, decimals),
		TokenSource: uint16(db.NetworkSolana),
	})
}

func solNewLendingBorrowRepayEvent(
	ctx *solContext,
	owner string,
	transferIx *db.SolanaInnerInstruction,
	direction db.EventTransferDirection,
	app, method string,
) {
	if !slices.Contains(ctx.wallets, owner) {
		return
	}

	t, ok := solParseTokenIxTokenTransfer(transferIx.Accounts, transferIx.Data)
	if !ok || (t.ix != solTokenIxTransfer && t.ix != solTokenIxTransferChecked) {
		return
	}

	var toWallet, fromWallet, userAccountAdress string

	switch direction {
	// incoming
	case db.EventTransferIncoming:
		toWallet = owner
		userAccountAdress = t.to
	// outgoing
	case db.EventTransferOutgoing:
		fromWallet = owner
		userAccountAdress = t.from
	}

	_, account, ok := solAccountExactOrError[solTokenAccountData](
		ctx, userAccountAdress,
	)
	if !ok {
		return
	}

	decimals, ok := solDecimals(ctx, account.Mint)
	if !ok {
		return
	}

	event := solNewEvent(ctx, app, method, db.EventTypeBorrowRepay)
	event.Transfers = append(event.Transfers, &db.EventTransfer{
		Direction:   direction,
		FromWallet:  fromWallet,
		FromAccount: t.from,
		ToWallet:    toWallet,
		ToAccount:   t.to,
		Token:       account.Mint,
		Amount:      newDecimalFromRawAmount(t.amount, decimals),
		TokenSource: uint16(db.NetworkSolana),
	})
}
