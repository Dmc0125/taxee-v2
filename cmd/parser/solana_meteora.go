package parser

import (
	"encoding/binary"
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

const (
	SOL_METEORA_POOLS_PROGRAM_ADDRESS = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB"
	SOL_METEORA_FARMS_PROGRAM_ADDRESS = "FarmuwXPWXvefWUeqFAa5w6rifLkq5X6E8bimYvrhCB1"
	SOL_MERCURIAL_PROGRAM_ADDRESS     = "MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky"
)

func solProcessMeteoraPoolsIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	var method, user string
	var eventType db.EventType

	switch disc {
	// AddImbalanceLiquidity
	case [8]uint8{79, 35, 122, 84, 173, 15, 93, 191}:
		user = ix.Accounts[13]
		method, eventType = "add_liquidity", db.EventTypeAddLiquidity
	// RemoveSingleSide
	case [8]uint8{84, 84, 177, 66, 254, 185, 10, 251}:
		user = ix.Accounts[12]
		method, eventType = "remove_liquidity", db.EventTypeRemoveLiquidity
	default:
		return
	}

	if !ctx.walletOwned(user) {
		return
	}

	solSwapEventFromTransfers(
		ctx, ix.InnerInstructions, events,
		"meteora_pools", method, eventType,
	)
}

var solMeteoraFarmsIxInitAccount = [8]uint8{108, 227, 130, 130, 252, 109, 75, 218}

func solPreprocessMeteoraFarmsIx(ctx *solContext, ix *db.SolanaInstruction) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	switch disc {
	case solMeteoraFarmsIxInitAccount:
		owner := ix.Accounts[2]
		if !ctx.walletOwned(owner) {
			return
		}

		innerIxIter := solInnerIxIterator{innerIxs: ix.InnerInstructions}
		_, to, amount, err := solAnchorInitAccountValidate(&innerIxIter)
		assert.NoErr(err, "")

		ctx.receiveSol(to, amount)
		ctx.init(to, true, nil)
	}
}

func solMeteoraFarmsNewStakingEvent(
	ctx *solContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
	app,
	tokenAccountAddress, stakeAccountAddress string,
	stake bool,
) {
	tokenAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, tokenAccountAddress)
	if tokenAccount == nil {
		return
	}
	tokenAccountData, ok := solAccountDataMust[solTokenAccountData](
		ctx, tokenAccount, tokenAccountAddress,
	)
	if !ok {
		return
	}

	stakeAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, stakeAccountAddress)
	if stakeAccount == nil {
		err := solNewErrMissingAccount(ctx, stakeAccountAddress)
		ctx.errors = append(ctx.errors, err)
		return
	}

	transferIx := ix.InnerInstructions[0]
	amount := binary.LittleEndian.Uint64(transferIx.Data[1:])
	decimals := solDecimalsMust(ctx, tokenAccountData.Mint)

	var method, fromAccount, toAccount string
	if stake {
		method = "stake"
		fromAccount, toAccount = tokenAccountAddress, stakeAccountAddress
	} else {
		method = "unstake"
		toAccount, fromAccount = tokenAccountAddress, stakeAccountAddress
	}

	event := solNewEvent(ctx)
	event.UiAppName = app
	event.UiMethodName = method
	event.Type = db.EventTypeTransfer
	event.Data = &db.EventTransfer{
		Direction:   db.EventTransferInternal,
		FromWallet:  tokenAccountData.Owner,
		ToWallet:    tokenAccountData.Owner,
		FromAccount: fromAccount,
		ToAccount:   toAccount,
		Token:       tokenAccountData.Mint,
		Amount:      newDecimalFromRawAmount(amount, decimals),
		TokenSource: uint16(db.NetworkSolana),
	}

	*events = append(*events, event)
}

func solProcessMeteoraFarmsIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	disc, ok := solAnchorDisc(ix.Data)
	if !ok {
		return
	}

	const app = "meteora_farms"

	switch disc {
	case solMeteoraFarmsIxInitAccount:
		owner := ix.Accounts[2]
		if !ctx.walletOwned(owner) {
			return
		}

		innerIxIter := solInnerIxIterator{innerIxs: ix.InnerInstructions}
		event, ok, err := solProcessAnchorInitAccount(ctx, &innerIxIter)
		assert.NoErr(err, "")
		if !ok {
			return
		}

		event.UiAppName = app
		event.UiMethodName = "create_user"

		*events = append(*events, event)
	// stake
	case [8]uint8{242, 35, 198, 137, 82, 225, 242, 182}:
		tokenAccountAddress, stakeAccountAddress := ix.Accounts[4], ix.Accounts[2]
		solMeteoraFarmsNewStakingEvent(
			ctx, ix, events,
			app,
			tokenAccountAddress, stakeAccountAddress,
			true,
		)
	// unstake
	case [8]uint8{183, 18, 70, 156, 148, 109, 161, 34}:
		tokenAccountAddress, stakeAccountAddress := ix.Accounts[4], ix.Accounts[2]
		solMeteoraFarmsNewStakingEvent(
			ctx, ix, events,
			app,
			tokenAccountAddress, stakeAccountAddress,
			false,
		)
	// claim
	case [8]uint8{62, 198, 214, 193, 213, 159, 108, 210}:
		transferIx := ix.InnerInstructions[0]
		amount, _, to := solParseTokenTransfer(
			transferIx.Accounts,
			transferIx.Data,
		)

		toAccount := ctx.findOwned(ctx.slot, ctx.ixIdx, to)
		if toAccount == nil {
			return
		}
		toAccountData, ok := solAccountDataMust[solTokenAccountData](
			ctx, toAccount, to,
		)
		if !ok {
			return
		}

		decimals := solDecimalsMust(ctx, toAccountData.Mint)

		event := solNewEvent(ctx)
		event.UiAppName = app
		event.UiMethodName = "claim"
		event.Type = db.EventTypeTransfer
		event.Data = &db.EventTransfer{
			Direction:   db.EventTransferIncoming,
			ToWallet:    toAccountData.Owner,
			ToAccount:   to,
			Token:       toAccountData.Mint,
			Amount:      newDecimalFromRawAmount(amount, decimals),
			TokenSource: uint16(db.NetworkSolana),
		}

		*events = append(*events, event)
	}
}

func solProcessMercurialIx(
	ctx *solContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	if len(ix.Data) == 0 {
		return
	}

	var method string
	var eventType db.EventType

	switch ix.Data[0] {
	// AddLiquidity
	case 1:
		method = "add_liquidity"
		eventType = db.EventTypeAddLiquidity
	case 3:
		method = "remove_liquidity"
		eventType = db.EventTypeRemoveLiquidity
	default:
		return
	}

	wallet, incomingTransfers, outgoingTransfers := solParseTransfers(
		ctx,
		ix.InnerInstructions,
	)

	if len(incomingTransfers) == 0 && len(outgoingTransfers) == 0 {
		return
	}

	event := solNewEvent(ctx)
	event.UiAppName = "mercurial"
	event.UiMethodName = method
	event.Type = eventType
	event.Data = &db.EventSwap{
		Wallet:   wallet,
		Incoming: incomingTransfers,
		Outgoing: outgoingTransfers,
	}

	*events = append(*events, event)
}
