package parser

import (
	"encoding/binary"
	"math"
	"slices"
	"taxee/pkg/db"

	"github.com/shopspring/decimal"
)

const (
	// V4
	evm1inchV4FillOrderRFQTo            uint32 = 0xbaba5855 // fillOrderRFQTo((uint256,address,address,address,address,uint256,uint256),bytes,uint256,uint256,address)
	evm1inchV4RenounceOwnership         uint32 = 0x715018a6 // renounceOwnership()
	evm1inchV4RescueFunds               uint32 = 0x78e3214f // rescueFunds(address,uint256)
	evm1inchV4Swap                      uint32 = 0x7c025200 // swap(address,(address,address,address,address,uint256,uint256,uint256,bytes),bytes)
	evm1inchV4UniswapV3Swap             uint32 = 0xe449022e // uniswapV3Swap(uint256,uint256,uint256[])
	evm1inchV4UniswapV3SwapCallback     uint32 = 0xfa461e33 // uniswapV3SwapCallback(int256,int256,bytes)
	evm1inchV4FillOrderRFQ              uint32 = 0xd0a3b665 // fillOrderRFQ((uint256,address,address,address,address,uint256,uint256),bytes,uint256,uint256)
	evm1inchV4FillOrderRFQToWithPermit  uint32 = 0x4cc4a27b // fillOrderRFQToWithPermit((uint256,address,address,address,address,uint256,uint256),bytes,uint256,uint256,address,bytes)
	evm1inchV4InvalidatorForOrderRFQ    uint32 = 0x56f16124 // invalidatorForOrderRFQ(address,uint256)
	evm1inchV4Owner                     uint32 = 0x8da5cb5b // owner()
	evm1inchV4TransferOwnership         uint32 = 0xf2fde38b // transferOwnership(address)
	evm1inchV4DOMAIN_SEPARATOR          uint32 = 0x3644e515 // DOMAIN_SEPARATOR()
	evm1inchV4Destroy                   uint32 = 0x83197ef0 // destroy()
	evm1inchV4Unoswap                   uint32 = 0x2e95b6c8 // unoswap(address,uint256,uint256,bytes32[])
	evm1inchV4LIMIT_ORDER_RFQ_TYPEHASH  uint32 = 0x6bf53d0  // LIMIT_ORDER_RFQ_TYPEHASH()
	evm1inchV4CancelOrderRFQ            uint32 = 0x825caba1 // cancelOrderRFQ(uint256)
	evm1inchV4UniswapV3SwapTo           uint32 = 0xbc80f1a8 // uniswapV3SwapTo(address,uint256,uint256,uint256[])
	evm1inchV4UniswapV3SwapToWithPermit uint32 = 0x2521b930 // uniswapV3SwapToWithPermit(address,address,uint256,uint256,uint256[],bytes)
	evm1inchV4UnoswapWithPermit         uint32 = 0xa1251d75 // unoswapWithPermit(address,uint256,uint256,bytes32[],bytes)

	// VUnknown
	evm1inchVUnknownRemaining                    uint32 = 0xbc1ed74c // remaining(bytes32)
	evm1inchVUnknownClipperSwapToWithPermit      uint32 = 0xc805a666 // clipperSwapToWithPermit(address,address,address,address,uint256,uint256,uint256,bytes32,bytes32,bytes)
	evm1inchVUnknownDestroy                      uint32 = 0x83197ef0 // destroy()
	evm1inchVUnknownFillOrderTo                  uint32 = 0xe5d7bde6 // fillOrderTo((uint256,address,address,address,address,address,uint256,uint256,uint256,bytes),bytes,bytes,uint256,uint256,uint256,address)
	evm1inchVUnknownFillOrderToWithPermit        uint32 = 0xd365c695 // fillOrderToWithPermit((uint256,address,address,address,address,address,uint256,uint256,uint256,bytes),bytes,bytes,uint256,uint256,uint256,address,bytes)
	evm1inchVUnknownNonce                        uint32 = 0x70ae92d2 // nonce(address)
	evm1inchVUnknownRemainingRaw                 uint32 = 0x7e54f092 // remainingRaw(bytes32)
	evm1inchVUnknownAnd                          uint32 = 0xbfa75143 // and(uint256,bytes)
	evm1inchVUnknownCancelOrder                  uint32 = 0x2d9a56f6 // cancelOrder((uint256,address,address,address,address,address,uint256,uint256,uint256,bytes))
	evm1inchVUnknownClipperSwap                  uint32 = 0x84bd6d29 // clipperSwap(address,address,address,uint256,uint256,uint256,bytes32,bytes32)
	evm1inchVUnknownFillOrderRFQTo               uint32 = 0x5a099843 // fillOrderRFQTo((uint256,address,address,address,address,uint256,uint256),bytes,uint256,address)
	evm1inchVUnknownRenounceOwnership            uint32 = 0x715018a6 // renounceOwnership()
	evm1inchVUnknownUniswapV3SwapToWithPermit    uint32 = 0x2521b930 // uniswapV3SwapToWithPermit(address,address,uint256,uint256,uint256[],bytes)
	evm1inchVUnknownAdvanceNonce                 uint32 = 0x72c244a8 // advanceNonce(uint8)
	evm1inchVUnknownRescueFunds                  uint32 = 0x78e3214f // rescueFunds(address,uint256)
	evm1inchVUnknownSimulate                     uint32 = 0xbd61951d // simulate(address,bytes)
	evm1inchVUnknownUniswapV3Swap                uint32 = 0xe449022e // uniswapV3Swap(uint256,uint256,uint256[])
	evm1inchVUnknownUnoswap                      uint32 = 0x502b1c5  // unoswap(address,uint256,uint256,uint256[])
	evm1inchVUnknownUnoswapTo                    uint32 = 0xf78dc253 // unoswapTo(address,address,uint256,uint256,uint256[])
	evm1inchVUnknownUnoswapToWithPermit          uint32 = 0x3c15fd91 // unoswapToWithPermit(address,address,uint256,uint256,uint256[],bytes)
	evm1inchVUnknownCancelOrderRFQ               uint32 = 0xbddccd35 // cancelOrderRFQ(uint256,uint256)
	evm1inchVUnknownEq                           uint32 = 0x6fe7b0ba // eq(uint256,bytes)
	evm1inchVUnknownFillOrder                    uint32 = 0x62e238bb // fillOrder((uint256,address,address,address,address,address,uint256,uint256,uint256,bytes),bytes,bytes,uint256,uint256,uint256)
	evm1inchVUnknownIncreaseNonce                uint32 = 0xc53a0292 // increaseNonce()
	evm1inchVUnknownLt                           uint32 = 0xca4ece22 // lt(uint256,bytes)
	evm1inchVUnknownSwap                         uint32 = 0x12aa3caf // swap(address,(address,address,address,address,uint256,uint256,uint256),bytes,bytes)
	evm1inchVUnknownArbitraryStaticCall          uint32 = 0xbf15fcd8 // arbitraryStaticCall(address,bytes)
	evm1inchVUnknownCheckPredicate               uint32 = 0x6c838250 // checkPredicate((uint256,address,address,address,address,address,uint256,uint256,uint256,bytes))
	evm1inchVUnknownClipperSwapTo                uint32 = 0x93d4fa5  // clipperSwapTo(address,address,address,address,uint256,uint256,uint256,bytes32,bytes32)
	evm1inchVUnknownGt                           uint32 = 0x4f38e2b8 // gt(uint256,bytes)
	evm1inchVUnknownTimestampBelowAndNonceEquals uint32 = 0x2cc2878d // timestampBelowAndNonceEquals(uint256)
	evm1inchVUnknownUniswapV3SwapTo              uint32 = 0xbc80f1a8 // uniswapV3SwapTo(address,uint256,uint256,uint256[])
	evm1inchVUnknownHashOrder                    uint32 = 0x37e7316f // hashOrder((uint256,address,address,address,address,address,uint256,uint256,uint256,bytes))
	evm1inchVUnknownInvalidatorForOrderRFQ       uint32 = 0x56f16124 // invalidatorForOrderRFQ(address,uint256)
	evm1inchVUnknownOr                           uint32 = 0x74261145 // or(uint256,bytes)
	evm1inchVUnknownOwner                        uint32 = 0x8da5cb5b // owner()
	evm1inchVUnknownFillOrderRFQToWithPermit     uint32 = 0x70ccbd31 // fillOrderRFQToWithPermit((uint256,address,address,address,address,uint256,uint256),bytes,uint256,address,bytes)
	evm1inchVUnknownNonceEquals                  uint32 = 0xcf6fc6e3 // nonceEquals(address,uint256)
	evm1inchVUnknownTimestampBelow               uint32 = 0x63592c2b // timestampBelow(uint256)
	evm1inchVUnknownTransferOwnership            uint32 = 0xf2fde38b // transferOwnership(address)
	evm1inchVUnknownUniswapV3SwapCallback        uint32 = 0xfa461e33 // uniswapV3SwapCallback(int256,int256,bytes)
	evm1inchVUnknownFillOrderRFQ                 uint32 = 0x3eca9c0a // fillOrderRFQ((uint256,address,address,address,address,uint256,uint256),bytes,uint256)
	evm1inchVUnknownFillOrderRFQCompact          uint32 = 0x9570eeee // fillOrderRFQCompact((uint256,address,address,address,address,uint256,uint256),bytes32,bytes32,uint256)
	evm1inchVUnknownRemainingsRaw                uint32 = 0x942461bb // remainingsRaw(bytes32[])

	evm1inchV4ContractId       uint32 = 0x4365f2fa
	evm1inchVUnknownContractId uint32 = 0x91bd1aa3
)

var evm1inchV4Selectors = map[uint32]bool{
	evm1inchV4LIMIT_ORDER_RFQ_TYPEHASH:  true,
	evm1inchV4FillOrderRFQ:              true,
	evm1inchV4FillOrderRFQToWithPermit:  true,
	evm1inchV4Destroy:                   true,
	evm1inchV4FillOrderRFQTo:            true,
	evm1inchV4InvalidatorForOrderRFQ:    true,
	evm1inchV4UnoswapWithPermit:         true,
	evm1inchV4DOMAIN_SEPARATOR:          true,
	evm1inchV4Owner:                     true,
	evm1inchV4RescueFunds:               true,
	evm1inchV4Swap:                      true,
	evm1inchV4UniswapV3Swap:             true,
	evm1inchV4UniswapV3SwapCallback:     true,
	evm1inchV4CancelOrderRFQ:            true,
	evm1inchV4RenounceOwnership:         true,
	evm1inchV4TransferOwnership:         true,
	evm1inchV4UniswapV3SwapTo:           true,
	evm1inchV4UniswapV3SwapToWithPermit: true,
	evm1inchV4Unoswap:                   true,
}

var evm1inchVUnknownSelectors = map[uint32]bool{
	evm1inchVUnknownRemaining:                    true,
	evm1inchVUnknownClipperSwapToWithPermit:      true,
	evm1inchVUnknownDestroy:                      true,
	evm1inchVUnknownFillOrderTo:                  true,
	evm1inchVUnknownFillOrderToWithPermit:        true,
	evm1inchVUnknownNonce:                        true,
	evm1inchVUnknownRemainingRaw:                 true,
	evm1inchVUnknownAnd:                          true,
	evm1inchVUnknownCancelOrder:                  true,
	evm1inchVUnknownClipperSwap:                  true,
	evm1inchVUnknownFillOrderRFQTo:               true,
	evm1inchVUnknownRenounceOwnership:            true,
	evm1inchVUnknownUniswapV3SwapToWithPermit:    true,
	evm1inchVUnknownAdvanceNonce:                 true,
	evm1inchVUnknownRescueFunds:                  true,
	evm1inchVUnknownSimulate:                     true,
	evm1inchVUnknownUniswapV3Swap:                true,
	evm1inchVUnknownUnoswap:                      true,
	evm1inchVUnknownUnoswapTo:                    true,
	evm1inchVUnknownUnoswapToWithPermit:          true,
	evm1inchVUnknownCancelOrderRFQ:               true,
	evm1inchVUnknownEq:                           true,
	evm1inchVUnknownFillOrder:                    true,
	evm1inchVUnknownIncreaseNonce:                true,
	evm1inchVUnknownLt:                           true,
	evm1inchVUnknownSwap:                         true,
	evm1inchVUnknownArbitraryStaticCall:          true,
	evm1inchVUnknownCheckPredicate:               true,
	evm1inchVUnknownClipperSwapTo:                true,
	evm1inchVUnknownGt:                           true,
	evm1inchVUnknownTimestampBelowAndNonceEquals: true,
	evm1inchVUnknownUniswapV3SwapTo:              true,
	evm1inchVUnknownHashOrder:                    true,
	evm1inchVUnknownInvalidatorForOrderRFQ:       true,
	evm1inchVUnknownOr:                           true,
	evm1inchVUnknownOwner:                        true,
	evm1inchVUnknownFillOrderRFQToWithPermit:     true,
	evm1inchVUnknownNonceEquals:                  true,
	evm1inchVUnknownTimestampBelow:               true,
	evm1inchVUnknownTransferOwnership:            true,
	evm1inchVUnknownUniswapV3SwapCallback:        true,
	evm1inchVUnknownFillOrderRFQ:                 true,
	evm1inchVUnknownFillOrderRFQCompact:          true,
	evm1inchVUnknownRemainingsRaw:                true,
}

func evmProcess1InchVUknownTx(
	ctx *evmContext,
	events *[]*db.Event,
	tx *db.EvmTransactionData,
) {
	selector := binary.BigEndian.Uint32(tx.Input[:4])

	switch selector {
	case evm1inchVUnknownSwap:
		sender := tx.From

		if !evmWalletOwned(ctx, sender) {
			return
		}

		amounts := make(map[string]decimal.Decimal)

		if !tx.Value.Equal(decimal.Zero) {
			amounts["ethereum"] = tx.Value.Neg()
		}

		// native amount
		// logs
		// internal

		for _, log := range tx.Events {
			if slices.Equal(log.Topics[0], evmErc20TransferTopic[:]) {
				from := evmAddressFrom32Bytes(log.Topics[1])
				to := evmAddressFrom32Bytes(log.Topics[2])
				amount := evmAmountFrom32Bytes(log.Data[:32])

				if sender == from {
					amounts[log.Address] = amounts[log.Address].Sub(amount)
				} else if sender == to {
					amounts[log.Address] = amounts[log.Address].Add(amount)
				}
			}
		}

		swapData := db.EventSwap{
			Wallet: sender,
		}

		for token, amount := range amounts {
			if amount.Equal(decimal.Zero) {
				continue
			}

			t := db.EventSwapTransfer{
				Account:     sender,
				Token:       token,
				Amount:      amount.Abs(),
				TokenSource: uint16(ctx.network),
			}
			if token == "ethereum" {
				t.TokenSource = math.MaxUint16
			}

			if amount.LessThan(decimal.Zero) {
				swapData.Outgoing = append(swapData.Outgoing, &t)
			} else if amount.GreaterThan(decimal.Zero) {
				swapData.Incoming = append(swapData.Incoming, &t)
			}
		}

		event := evmNewEvent(ctx)
		event.UiAppName = "1inch_aggregator"
		event.UiMethodName = "swap"
		event.Type = db.EventTypeSwap
		event.Data = &swapData

		*events = append(*events, event)
	}
}
