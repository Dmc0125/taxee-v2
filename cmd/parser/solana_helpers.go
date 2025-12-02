package parser

import (
	"errors"
	"fmt"
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
		ixType, _, ok := solSystemIxFromData(transferIx.Data)
		if !ok {
			// err
			return
		}

		switch ixType {
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

		payer, tokenAccount, amount, ok = solParseSystemIxSolTransfer(
			ixType,
			transferIx.Accounts,
			transferIx.Data,
		)
		if !ok {
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

	ixType, _, ok := solSystemIxFromData(firstIx.Data)
	if !ok {
		err = fmt.Errorf("invalid anchor init ix: %#v", *firstIx)
		return
	}

	switch ixType {
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
) (*db.Event, bool, error) {
	from, to, amount, err := solAnchorInitAccountValidate(innerIxs)
	if err != nil {
		return nil, false, err
	}

	// NOTE: can only happen if unknown account is being created (squads tx)
	fromInternal := ctx.walletOwned(from)
	toInternal := slices.Contains(ctx.wallets, to) ||
		ctx.findOwned(ctx.slot, ctx.ixIdx, to) != nil
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
					amounts[validMint] = amounts[validMint].Sub(
						newDecimalFromRawAmount(amount, decimals),
					)
					tokenAccounts[validMint] = from
				}
			}
			if to != "" {
				if validMint, wallet, ok = getTokenAccount(to, mint); ok {
					decimals := solDecimalsMust(ctx, validMint)
					amounts[validMint] = amounts[validMint].Add(
						newDecimalFromRawAmount(amount, decimals),
					)
					tokenAccounts[validMint] = to
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
	events *[]*db.Event,
	app, method string,
	eventType db.EventType,
) {
	transfers, outgoingCount, incomingCount := solParseTransfers(ctx, innerInstructions)
	if outgoingCount == 0 || incomingCount == 0 {
		err := solNewError(ctx)
		err.Type = db.ParserErrorTypeOneSidedSwap
		return
	}

	event := solNewEvent(ctx)
	event.App = app
	event.Method = method
	event.Type = eventType
	event.Transfers = transfers

	*events = append(*events, event)
}

func solNewLendingDepositWithdrawEvent(
	ctx *solContext,
	events *[]*db.Event,
	owner, userAddress string,
	transferIx *db.SolanaInnerInstruction,
	app string,
) {
	if !slices.Contains(ctx.wallets, owner) {
		return
	}

	if _, ok := ctx.findOwnedOrError(ctx.slot, ctx.ixIdx, userAddress); !ok {
		return
	}

	amount, from, _ := solParseTokenTransfer(transferIx.Accounts, transferIx.Data)
	_, tokenAccount, ok := solAccountExactOrError[solTokenAccountData](ctx, from)
	if !ok {
		return
	}

	decimals := solDecimalsMust(ctx, tokenAccount.Mint)

	event := solNewEvent(ctx)
	event.App = app
	event.Method = "deposit"
	event.Type = db.EventTypeTransfer
	event.Transfers = append(event.Transfers, &db.EventTransfer{
		Direction:   db.EventTransferInternal,
		FromWallet:  owner,
		FromAccount: from,
		ToWallet:    owner,
		ToAccount:   userAddress,
		Token:       tokenAccount.Mint,
		Amount:      newDecimalFromRawAmount(amount, decimals),
		TokenSource: uint16(db.NetworkSolana),
	})

	*events = append(*events, event)
}

func solNewLendingBorrowRepayEvent(
	ctx *solContext,
	events *[]*db.Event,
	owner string,
	transferIx *db.SolanaInnerInstruction,
	direction db.EventTransferDirection,
	app, method string,
) {
	if !slices.Contains(ctx.wallets, owner) {
		return
	}

	var amount uint64
	var fromWallet, toWallet, fromAccount, toAccount, userAccountAdress string

	switch direction {
	// incoming
	case db.EventTransferIncoming:
		amount, _, toAccount = solParseTokenTransfer(
			transferIx.Accounts, transferIx.Data,
		)
		toWallet = owner
		userAccountAdress = toAccount
	// outgoing
	case db.EventTransferOutgoing:
		amount, fromAccount, _ = solParseTokenTransfer(
			transferIx.Accounts, transferIx.Data,
		)
		fromWallet = owner
		userAccountAdress = fromAccount
	}

	_, account, ok := solAccountExactOrError[solTokenAccountData](
		ctx, userAccountAdress,
	)
	if !ok {
		return
	}

	decimals := solDecimalsMust(ctx, account.Mint)

	event := solNewEvent(ctx)
	event.App = app
	event.Method = method
	event.Type = db.EventTypeBorrowRepay
	event.Transfers = append(event.Transfers, &db.EventTransfer{
		Direction:   direction,
		FromWallet:  fromWallet,
		FromAccount: fromAccount,
		ToWallet:    toWallet,
		ToAccount:   toAccount,
		Token:       account.Mint,
		Amount:      newDecimalFromRawAmount(amount, decimals),
		TokenSource: uint16(db.NetworkSolana),
	})

	*events = append(*events, event)
}
