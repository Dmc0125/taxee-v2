package parser

import (
	"encoding/binary"
	"taxee/pkg/db"
)

const evmArbitrumDistributorContractAddress = "0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9"

const (
	evmArbitrumDistributorClaim uint32 = 0x4e71d92d // claim()
	// evmArbitrumDistributorClaimPeriodStart  uint32 = 0x58c13b7e // claimPeriodStart()
	// evmArbitrumDistributorClaimableTokens   uint32 = 0x84d24226 // claimableTokens(address)
	// evmArbitrumDistributorRenounceOwnership uint32 = 0x715018a6 // renounceOwnership()
	// evmArbitrumDistributorClaimPeriodEnd    uint32 = 0x3da082a0 // claimPeriodEnd()
	// evmArbitrumDistributorSetSweepReciever  uint32 = 0xb438abde // setSweepReciever(address)
	// evmArbitrumDistributorSweep             uint32 = 0x35faa416 // sweep()
	// evmArbitrumDistributorSweepReceiver     uint32 = 0xf6e0df9f // sweepReceiver()
	// evmArbitrumDistributorTransferOwnership uint32 = 0xf2fde38b // transferOwnership(address)
	// evmArbitrumDistributorClaimAndDelegate  uint32 = 0x78e2b594 // claimAndDelegate(address,uint256,uint8,bytes32,bytes32)
	// evmArbitrumDistributorOwner             uint32 = 0x8da5cb5b // owner()
	// evmArbitrumDistributorSetRecipients     uint32 = 0xae373c1b // setRecipients(address[],uint256[])
	// evmArbitrumDistributorTotalClaimable    uint32 = 0x4838ed19 // totalClaimable()
	// evmArbitrumDistributorToken             uint32 = 0xfc0c546a // token()
	// evmArbitrumDistributorWithdraw          uint32 = 0x2e1a7d4d // withdraw(uint256)

	// evmArbitrumDistributorContractId uint32 = 0x9a138f45
)

// var evmArbitrumDistributorSelectors = map[uint32]bool{
// 	evmArbitrumDistributorClaim:             true,
// 	evmArbitrumDistributorClaimPeriodStart:  true,
// 	evmArbitrumDistributorClaimableTokens:   true,
// 	evmArbitrumDistributorRenounceOwnership: true,
// 	evmArbitrumDistributorClaimPeriodEnd:    true,
// 	evmArbitrumDistributorSetSweepReciever:  true,
// 	evmArbitrumDistributorSweep:             true,
// 	evmArbitrumDistributorSweepReceiver:     true,
// 	evmArbitrumDistributorTransferOwnership: true,
// 	evmArbitrumDistributorClaimAndDelegate:  true,
// 	evmArbitrumDistributorOwner:             true,
// 	evmArbitrumDistributorSetRecipients:     true,
// 	evmArbitrumDistributorTotalClaimable:    true,
// 	evmArbitrumDistributorToken:             true,
// 	evmArbitrumDistributorWithdraw:          true,
// }

func evmProcessArbitrumDistributorTx(
	ctx *evmContext,
	events *[]*db.Event,
	tx *db.EvmTransactionData,
) {
	selector := binary.BigEndian.Uint32(tx.Input[:4])

	switch selector {
	case evmArbitrumDistributorClaim:
		sender := tx.From
		if !evmWalletOwned(ctx, sender) {
			return
		}

		event := evmNewEvent(ctx)
		event.App = "arbitrum_distributor"
		event.Method = "claim"
		event.Type = db.EventTypeTransfer

		transferLog := tx.Events[0]
		amount := evmAmountFrom32Bytes(transferLog.Data[:32])

		event.Transfers = append(event.Transfers, &db.EventTransfer{
			Direction:   db.EventTransferIncoming,
			ToWallet:    sender,
			ToAccount:   sender,
			Token:       transferLog.Address,
			Amount:      amount,
			TokenSource: uint16(db.NetworkArbitrum),
		})

		*events = append(*events, event)
	}
}
