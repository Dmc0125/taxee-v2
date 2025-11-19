package parser

const (
	evmTraderJoeRouter2GetLegacyFactory                                      uint32 = 0x71d1974a // getLegacyFactory()
	evmTraderJoeRouter2SwapExactNATIVEForTokens                              uint32 = 0xb066ea7c // swapExactNATIVEForTokens(uint256,(uint256[],uint8[],address[]),address,uint256)
	evmTraderJoeRouter2SwapExactTokensForNATIVESupportingFeeOnTransferTokens uint32 = 0x1a24f9a9 // swapExactTokensForNATIVESupportingFeeOnTransferTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
	evmTraderJoeRouter2GetWNATIVE                                            uint32 = 0x6c9c0078 // getWNATIVE()
	evmTraderJoeRouter2AddLiquidity                                          uint32 = 0xa3c7271a // addLiquidity((address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,int256[],uint256[],uint256[],address,address,uint256))
	evmTraderJoeRouter2AddLiquidityNATIVE                                    uint32 = 0x8efc2b2c // addLiquidityNATIVE((address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,int256[],uint256[],uint256[],address,address,uint256))
	evmTraderJoeRouter2GetIdFromPrice                                        uint32 = 0xf96fe925 // getIdFromPrice(address,uint256)
	evmTraderJoeRouter2GetSwapIn                                             uint32 = 0x964f987c // getSwapIn(address,uint128,bool)
	evmTraderJoeRouter2GetSwapOut                                            uint32 = 0xa0d376cf // getSwapOut(address,uint128,bool)
	evmTraderJoeRouter2GetV1Factory                                          uint32 = 0xbb558a9f // getV1Factory()
	evmTraderJoeRouter2SwapExactTokensForTokens                              uint32 = 0x2a443fae // swapExactTokensForTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
	evmTraderJoeRouter2GetLegacyRouter                                       uint32 = 0xba846523 // getLegacyRouter()
	evmTraderJoeRouter2GetPriceFromId                                        uint32 = 0xd0e380f2 // getPriceFromId(address,uint24)
	evmTraderJoeRouter2RemoveLiquidityNATIVE                                 uint32 = 0x81c2fdfb // removeLiquidityNATIVE(address,uint16,uint256,uint256,uint256[],uint256[],address,uint256)
	evmTraderJoeRouter2SwapExactTokensForTokensSupportingFeeOnTransferTokens uint32 = 0x4b801870 // swapExactTokensForTokensSupportingFeeOnTransferTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
	evmTraderJoeRouter2SwapTokensForExactNATIVE                              uint32 = 0x3dc8f8ec // swapTokensForExactNATIVE(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
	evmTraderJoeRouter2SwapTokensForExactTokens                              uint32 = 0x92fe8e70 // swapTokensForExactTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
	evmTraderJoeRouter2Sweep                                                 uint32 = 0x62c06767 // sweep(address,address,uint256)
	evmTraderJoeRouter2RemoveLiquidity                                       uint32 = 0xc22159b6 // removeLiquidity(address,address,uint16,uint256,uint256,uint256[],uint256[],address,uint256)
	evmTraderJoeRouter2CreateLBPair                                          uint32 = 0x659ac74b // createLBPair(address,address,uint24,uint16)
	evmTraderJoeRouter2GetFactory                                            uint32 = 0x88cc58e4 // getFactory()
	evmTraderJoeRouter2SwapExactNATIVEForTokensSupportingFeeOnTransferTokens uint32 = 0xe038e6dc // swapExactNATIVEForTokensSupportingFeeOnTransferTokens(uint256,(uint256[],uint8[],address[]),address,uint256)
	evmTraderJoeRouter2SwapExactTokensForNATIVE                              uint32 = 0x9ab6156b // swapExactTokensForNATIVE(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
	evmTraderJoeRouter2SwapNATIVEForExactTokens                              uint32 = 0x2075ad22 // swapNATIVEForExactTokens(uint256,(uint256[],uint8[],address[]),address,uint256)
	evmTraderJoeRouter2SweepLBToken                                          uint32 = 0xe9361c08 // sweepLBToken(address,address,uint256[],uint256[])

	evmTraderJoeRouter2ContractId uint32 = 0x35bfaf53
)

var evmTraderJoeRouter2Selectors = map[uint32]bool{
	evmTraderJoeRouter2GetLegacyFactory:                                      true,
	evmTraderJoeRouter2SwapExactNATIVEForTokens:                              true,
	evmTraderJoeRouter2SwapExactTokensForNATIVESupportingFeeOnTransferTokens: true,
	evmTraderJoeRouter2GetWNATIVE:                                            true,
	evmTraderJoeRouter2AddLiquidity:                                          true,
	evmTraderJoeRouter2AddLiquidityNATIVE:                                    true,
	evmTraderJoeRouter2GetIdFromPrice:                                        true,
	evmTraderJoeRouter2GetSwapIn:                                             true,
	evmTraderJoeRouter2GetSwapOut:                                            true,
	evmTraderJoeRouter2GetV1Factory:                                          true,
	evmTraderJoeRouter2SwapExactTokensForTokens:                              true,
	evmTraderJoeRouter2GetLegacyRouter:                                       true,
	evmTraderJoeRouter2GetPriceFromId:                                        true,
	evmTraderJoeRouter2RemoveLiquidityNATIVE:                                 true,
	evmTraderJoeRouter2SwapExactTokensForTokensSupportingFeeOnTransferTokens: true,
	evmTraderJoeRouter2SwapTokensForExactNATIVE:                              true,
	evmTraderJoeRouter2SwapTokensForExactTokens:                              true,
	evmTraderJoeRouter2Sweep:                                                 true,
	evmTraderJoeRouter2RemoveLiquidity:                                       true,
	evmTraderJoeRouter2CreateLBPair:                                          true,
	evmTraderJoeRouter2GetFactory:                                            true,
	evmTraderJoeRouter2SwapExactNATIVEForTokensSupportingFeeOnTransferTokens: true,
	evmTraderJoeRouter2SwapExactTokensForNATIVE:                              true,
	evmTraderJoeRouter2SwapNATIVEForExactTokens:                              true,
	evmTraderJoeRouter2SweepLBToken:                                          true,
}

// func evmProcessTraderJoeRouter2Tx(
// 	ctx *evmContext,
// 	events *[]*db.Event,
// 	tx *db.EvmTransactionData,
// ) {
// 	sender := tx.From
// 	if !evmWalletOwned(ctx, sender) {
// 		return
// 	}
//
// 	swapData := evmProcessSwap(
// 		sender, ctx.network,
// 		tx.Value, tx.InternalTxs, tx.Events,
// 	)
//
// 	if len(swapData.Outgoing) == 0 && len(swapData.Incoming) == 0 {
// 		return
// 	}
//
// 	event := evmNewEvent(ctx)
// 	event.UiAppName = "trader_joe"
// 	event.UiMethodName = "swap"
// 	event.Type = db.EventTypeSwap
// 	event.Data = &swapData
//
// 	*events = append(*events, event)
// }
