package parser

import (
	"encoding/binary"
	"slices"
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

const SOL_DRIFT_PROGRAM_ADDRESS = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"

var (
	solDriftIxInitializeUserStats = [8]uint8{254, 243, 72, 98, 251, 130, 168, 213}
	solDriftIxInitializeUser      = [8]uint8{111, 17, 185, 250, 60, 122, 38, 254}
)

func solPreprocessDriftIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	var owner string

	switch disc {
	case solDriftIxInitializeUserStats:
		owner = ix.Accounts[2]
	case solDriftIxInitializeUser:
		owner = ix.Accounts[3]
	default:
		return
	}

	if !ctx.walletOwned(owner) {
		return
	}

	innerIxs := solInnerIxIterator{innerIxs: ix.InnerInstructions}
	_, to, amount, err := solAnchorInitAccountValidate(&innerIxs)
	assert.NoErr(err, "")

	if innerIxs.hasNext() {
		transferIx := innerIxs.next()
		amount += binary.LittleEndian.Uint64(transferIx.Data[4:])
	}

	ctx.receiveSol(to, amount)
	ctx.init(to, true, nil)
}

func solDriftProcessInit(
	ctx *solContext,
	events *[]*db.Event,
	owner, payer string,
	innerIxs []*db.SolanaInnerInstruction,
	app, method string,
) bool {
	if !ctx.walletOwned(owner) {
		return false
	}
	payerInternal := ctx.walletOwned(payer)

	innerIxsIter := solInnerIxIterator{innerIxs, 0}
	_, to, amount, err := solAnchorInitAccountValidate(&innerIxsIter)
	assert.NoErr(err, "")

	if innerIxsIter.hasNext() {
		transferIx := innerIxsIter.next()
		amount += binary.LittleEndian.Uint64(transferIx.Data[4:])
	}

	event := solNewEvent(ctx)
	event.UiAppName = app
	event.UiMethodName = method
	event.Type = db.EventTypeTransfer
	event.Data = &db.EventTransfer{
		Direction:   getTransferEventDirection(payerInternal, true),
		FromWallet:  payer,
		ToWallet:    owner,
		FromAccount: payer,
		ToAccount:   to,
		Token:       SOL_MINT_ADDRESS,
		Amount:      newDecimalFromRawAmount(amount, 9),
		TokenSource: uint16(db.NetworkSolana),
	}

	*events = append(*events, event)

	return true
}

// type solDriftLendingAccount struct {
// 	deposits map[string]decimal.Decimal
// }
//
// func (a solDriftLendingAccount) name() string {
// 	return "drift_lending"
// }

// TODO: withdrawals should be handled differently, either with tracking balances
// with accounts or with inventory
func driftProcessBorrowLend(
	ctx *solContext,
	events *[]*db.Event,
	accounts []string,
	tokenAccountAddress string,
	innerInstructions []*db.SolanaInnerInstruction,
	direction int,
	app string,
) {
	owner := accounts[3]
	if !ctx.walletOwned(owner) {
		return
	}

	userAddress := accounts[1]
	_, ok := ctx.findOwnedOrError(ctx.slot, ctx.ixIdx, userAddress)
	if !ok {
		return
	}
	// _, userData, ok := solAccountExactOrError[solDriftLendingAccount](
	// 	ctx, userAddress,
	// )
	// if !ok {
	// 	return
	// }

	transferIx := innerInstructions[0]
	amount := binary.LittleEndian.Uint64(transferIx.Data[1:])

	_, tokenAccountData, ok := solAccountExactOrError[solTokenAccountData](
		ctx, tokenAccountAddress,
	)
	if !ok {
		return
	}

	decimals := solDecimalsMust(ctx, tokenAccountData.Mint)
	uiAmount := newDecimalFromRawAmount(amount, decimals)

	// switch direction {
	// case 0:
	// 	userData.deposits[tokenAccountData.Mint] = userData.deposits[tokenAccountData.Mint].Add(
	// 		uiAmount,
	// 	)
	// case 1:
	// 	balance := userData.deposits[tokenAccountData.Mint]
	// 	if balance.LessThan(uiAmount) {
	//
	// 	}
	// }

	var fromAccount, toAccount, method string

	switch direction {
	case 0:
		method = "lend"
		fromAccount, toAccount = tokenAccountAddress, userAddress
	case 1:
		method = "withdraw"
		fromAccount, toAccount = userAddress, tokenAccountAddress
	}

	event := solNewEvent(ctx)
	event.UiAppName = app
	event.UiMethodName = method
	event.Type = db.EventTypeTransfer
	event.Data = &db.EventTransfer{
		Direction:   db.EventTransferInternal,
		FromWallet:  owner,
		ToWallet:    owner,
		FromAccount: fromAccount,
		ToAccount:   toAccount,
		Token:       tokenAccountData.Mint,
		Amount:      uiAmount,
		TokenSource: uint16(db.NetworkSolana),
	}

	*events = append(*events, event)
}

func solProcessDriftIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "drift"

	switch disc {
	// Deposit (lend)
	case [8]uint8{242, 35, 198, 137, 82, 225, 242, 182}:
		tokenAccountAddress := ix.Accounts[5]
		driftProcessBorrowLend(
			ctx, events,
			ix.Accounts, tokenAccountAddress, ix.InnerInstructions,
			0, app,
		)
		return
	// Withdraw
	case [8]uint8{183, 18, 70, 156, 148, 109, 161, 34}:
		tokenAccountAddress := ix.Accounts[6]
		driftProcessBorrowLend(
			ctx, events,
			ix.Accounts, tokenAccountAddress, ix.InnerInstructions,
			1, app,
		)
		return
	// DepositIntoSpotMarketRevenuePool (fee ?)
	case [8]uint8{92, 40, 151, 42, 122, 254, 139, 246}:
		owner := ix.Accounts[2]
		if !slices.Contains(ctx.wallets, owner) {
			return
		}

		transferIx := ix.InnerInstructions[0]
		amount, from, to := solParseTokenTransfer(transferIx.Accounts, transferIx.Data)

		tokenAccount, ok := ctx.findOwnedOrError(ctx.slot, ctx.ixIdx, from)
		if !ok {
			return
		}
		tokenAccountData, ok := solAccountDataMust[solTokenAccountData](ctx, tokenAccount, from)
		if !ok {
			return
		}

		decimals := solDecimalsMust(ctx, tokenAccountData.Mint)

		event := solNewEvent(ctx)
		event.UiAppName = app
		event.UiMethodName = "fee"
		event.Type = db.EventTypeTransfer
		event.Data = &db.EventTransfer{
			Direction:   db.EventTransferOutgoing,
			FromWallet:  owner,
			ToWallet:    to,
			FromAccount: from,
			ToAccount:   to,
			Token:       tokenAccountData.Mint,
			Amount:      newDecimalFromRawAmount(amount, decimals),
			TokenSource: uint16(db.NetworkSolana),
		}

		*events = append(*events, event)
		return
	}

	var owner, payer, method string

	switch disc {
	case solDriftIxInitializeUserStats:
		owner, payer = ix.Accounts[2], ix.Accounts[3]
		method = "init_user_stats"
	case solDriftIxInitializeUser:
		owner, payer = ix.Accounts[3], ix.Accounts[4]
		method = "init_user"
	default:
		return
	}

	if ok = solDriftProcessInit(
		ctx, events,
		owner, payer, ix.InnerInstructions,
		app, method,
	); !ok {
		return
	}

	// if user != "" {
	// 	ctx.init(user, true, solDriftLendingAccount{
	// 		deposits: make(map[string]decimal.Decimal),
	// 	})
	// }
}
