package parser

import (
	"encoding/binary"
	"slices"
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

const SOL_DRIFT_PROGRAM_ADDRESS = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"

var (
	solDriftIxInitializeUserStats    = [8]uint8{254, 243, 72, 98, 251, 130, 168, 213}
	solDriftIxInitializeUser         = [8]uint8{111, 17, 185, 250, 60, 122, 38, 254}
	solDriftIxInitInsuranceFundStake = [8]uint8{187, 179, 243, 70, 248, 90, 92, 147}
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
	case solDriftIxInitInsuranceFundStake:
		owner = ix.Accounts[4]
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
	event.App = app
	event.Method = method
	event.Type = db.EventTypeTransfer
	event.Transfers = append(event.Transfers, &db.EventTransfer{
		Direction:   getTransferEventDirection(payerInternal, true),
		FromWallet:  payer,
		ToWallet:    owner,
		FromAccount: payer,
		ToAccount:   to,
		Token:       SOL_MINT_ADDRESS,
		Amount:      newDecimalFromRawAmount(amount, 9),
		TokenSource: uint16(db.NetworkSolana),
	})

	*events = append(*events, event)

	return true
}

func driftProcessBorrowLend(
	ctx *solContext,
	owner, driftAccountAddress, tokenAccountAddress string,
	innerInstructions []*db.SolanaInnerInstruction,
	// 0 => lend / stake
	// 1 => withdraw
	direction int,
) *db.EventTransfer {
	// owner := accounts[3]
	if !ctx.walletOwned(owner) {
		return nil
	}

	// userAddress := accounts[1]
	_, ok := ctx.findOwnedOrError(ctx.slot, ctx.ixIdx, driftAccountAddress)
	if !ok {
		return nil
	}

	transferIx := innerInstructions[0]
	amount := binary.LittleEndian.Uint64(transferIx.Data[1:])

	_, tokenAccountData, ok := solAccountExactOrError[solTokenAccountData](
		ctx, tokenAccountAddress,
	)
	if !ok {
		return nil
	}

	decimals := solDecimalsMust(ctx, tokenAccountData.Mint)
	uiAmount := newDecimalFromRawAmount(amount, decimals)

	var fromAccount, toAccount string
	switch direction {
	case 0:
		fromAccount, toAccount = tokenAccountAddress, driftAccountAddress
	case 1:
		fromAccount, toAccount = driftAccountAddress, tokenAccountAddress
	}

	return &db.EventTransfer{
		Direction:   db.EventTransferInternal,
		FromWallet:  owner,
		ToWallet:    owner,
		FromAccount: fromAccount,
		ToAccount:   toAccount,
		Token:       tokenAccountData.Mint,
		Amount:      uiAmount,
		TokenSource: uint16(db.NetworkSolana),
	}

	// event := solNewEvent(ctx)
	// event.App = app
	// event.Method = method
	// event.Type = db.EventTypeTransfer
	// event.Transfers = append(event.Transfers)
	//
	// *events = append(*events, event)
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
	var method string
	var transfer *db.EventTransfer

	switch disc {
	// Deposit (lend)
	case [8]uint8{242, 35, 198, 137, 82, 225, 242, 182}:
		owner := ix.Accounts[3]
		driftAccountAddress := ix.Accounts[1]
		tokenAccountAddress := ix.Accounts[5]

		method = "lend"
		transfer = driftProcessBorrowLend(
			ctx,
			owner, driftAccountAddress, tokenAccountAddress,
			ix.InnerInstructions, 0,
		)
	// Withdraw
	case [8]uint8{183, 18, 70, 156, 148, 109, 161, 34}:
		owner := ix.Accounts[3]
		driftAccountAddress := ix.Accounts[1]
		tokenAccountAddress := ix.Accounts[6]

		method = "withdraw"
		transfer = driftProcessBorrowLend(
			ctx,
			owner, driftAccountAddress, tokenAccountAddress,
			ix.InnerInstructions, 1,
		)
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

		method = "fee"
		transfer = &db.EventTransfer{
			Direction:   db.EventTransferOutgoing,
			FromWallet:  owner,
			ToWallet:    to,
			FromAccount: from,
			ToAccount:   to,
			Token:       tokenAccountData.Mint,
			Amount:      newDecimalFromRawAmount(amount, decimals),
			TokenSource: uint16(db.NetworkSolana),
		}
	// deposit stake
	case [8]uint8{251, 144, 115, 11, 222, 47, 62, 236}:
		owner := ix.Accounts[4]
		stakeAccountAddress := ix.Accounts[2]
		tokenAccountAddress := ix.Accounts[8]

		method = "if_stake"
		transfer = driftProcessBorrowLend(
			ctx,
			owner, stakeAccountAddress, tokenAccountAddress,
			ix.InnerInstructions, 0,
		)
	// withdraw stake
	case [8]uint8{128, 166, 142, 9, 254, 187, 143, 174}:
		owner := ix.Accounts[4]
		stakeAccountAddress := ix.Accounts[2]
		tokenAccountAddress := ix.Accounts[7]

		method = "if_unstake"
		transfer = driftProcessBorrowLend(
			ctx,
			owner, stakeAccountAddress, tokenAccountAddress,
			ix.InnerInstructions, 1,
		)
	}

	if transfer != nil {
		event := solNewEvent(ctx)
		event.App = app
		event.Method = method
		event.Type = db.EventTypeTransfer
		event.Transfers = append(event.Transfers, transfer)
		*events = append(*events, event)
		return
	}

	var owner, payer string

	switch disc {
	case solDriftIxInitializeUserStats:
		owner, payer = ix.Accounts[2], ix.Accounts[3]
		method = "init_user_stats"
	case solDriftIxInitializeUser:
		owner, payer = ix.Accounts[3], ix.Accounts[4]
		method = "init_user"
	case solDriftIxInitInsuranceFundStake:
		owner, payer = ix.Accounts[4], ix.Accounts[5]
		method = "init_insurance_fund"
	default:
		return
	}

	solDriftProcessInit(
		ctx, events,
		owner, payer, ix.InnerInstructions,
		app, method,
	)
}
