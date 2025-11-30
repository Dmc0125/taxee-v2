package parser

import (
	"encoding/binary"
	"taxee/pkg/db"
)

const (
	evmUniswapV3WETH9                        uint32 = 0x4aa4a4fc // WETH9()
	evmUniswapV3CallPositionManager          uint32 = 0xb3a2af13 // callPositionManager(bytes)
	evmUniswapV3Mint                         uint32 = 0x11ed56c9 // mint((address,address,uint24,int24,int24,uint256,uint256,address))
	evmUniswapV3Multicall                    uint32 = 0xac9650d8 // multicall(bytes[])
	evmUniswapV3SwapExactTokensForTokens     uint32 = 0x472b43f3 // swapExactTokensForTokens(uint256,uint256,address[],address)
	evmUniswapV3ApproveMaxMinusOne           uint32 = 0xcab372ce // approveMaxMinusOne(address)
	evmUniswapV3ApproveZeroThenMaxMinusOne   uint32 = 0xab3fdd50 // approveZeroThenMaxMinusOne(address)
	evmUniswapV3RefundETH                    uint32 = 0x12210e8a // refundETH()
	evmUniswapV3SweepToken                   uint32 = 0xe90a182f // sweepToken(address,uint256)
	evmUniswapV3SelfPermitIfNecessary        uint32 = 0xc2e3140a // selfPermitIfNecessary(address,uint256,uint256,uint8,bytes32,bytes32)
	evmUniswapV3ApproveMax                   uint32 = 0x571ac8b0 // approveMax(address)
	evmUniswapV3ExactInput                   uint32 = 0xb858183f // exactInput((bytes,address,uint256,uint256))
	evmUniswapV3ExactOutput                  uint32 = 0x9b81346  // exactOutput((bytes,address,uint256,uint256))
	evmUniswapV3Factory                      uint32 = 0xc45a0155 // factory()
	evmUniswapV3IncreaseLiquidity            uint32 = 0xf100b205 // increaseLiquidity((address,address,uint256,uint256,uint256))
	evmUniswapV3PositionManager              uint32 = 0x791b98bc // positionManager()
	evmUniswapV3WrapETH                      uint32 = 0x1c58db4f // wrapETH(uint256)
	evmUniswapV3ExactInputSingle             uint32 = 0x4e45aaf  // exactInputSingle((address,address,uint24,address,uint256,uint256,uint160))
	evmUniswapV3FactoryV2                    uint32 = 0x68e0d4e1 // factoryV2()
	evmUniswapV3GetApprovalType              uint32 = 0xdee00f35 // getApprovalType(address,uint256)
	evmUniswapV3UniswapV3SwapCallback        uint32 = 0xfa461e33 // uniswapV3SwapCallback(int256,int256,bytes)
	evmUniswapV3ExactOutputSingle            uint32 = 0x5023b4df // exactOutputSingle((address,address,uint24,address,uint256,uint256,uint160))
	evmUniswapV3ApproveZeroThenMax           uint32 = 0x639d71a9 // approveZeroThenMax(address)
	evmUniswapV3SelfPermit                   uint32 = 0xf3995c67 // selfPermit(address,uint256,uint256,uint8,bytes32,bytes32)
	evmUniswapV3SelfPermitAllowed            uint32 = 0x4659a494 // selfPermitAllowed(address,uint256,uint256,uint8,bytes32,bytes32)
	evmUniswapV3SweepTokenWithFee            uint32 = 0xe0e189a0 // sweepTokenWithFee(address,uint256,address,uint256,address)
	evmUniswapV3UnwrapWETH9                  uint32 = 0x49616997 // unwrapWETH9(uint256)
	evmUniswapV3UnwrapWETH9WithFee           uint32 = 0xd4ef38de // unwrapWETH9WithFee(uint256,uint256,address)
	evmUniswapV3SwapTokensForExactTokens     uint32 = 0x42712a67 // swapTokensForExactTokens(uint256,uint256,address[],address)
	evmUniswapV3CheckOracleSlippage          uint32 = 0xf25801a7 // checkOracleSlippage(bytes,uint24,uint32)
	evmUniswapV3Pull                         uint32 = 0xf2d5d56b // pull(address,uint256)
	evmUniswapV3SelfPermitAllowedIfNecessary uint32 = 0xa4a78f0c // selfPermitAllowedIfNecessary(address,uint256,uint256,uint8,bytes32,bytes32)

	evmUniswapV3ContractId uint32 = 0xb819dbd5
)

var evmUniswapV3Selectors = map[uint32]bool{
	evmUniswapV3WETH9:                        true,
	evmUniswapV3CallPositionManager:          true,
	evmUniswapV3Mint:                         true,
	evmUniswapV3Multicall:                    true,
	evmUniswapV3SwapExactTokensForTokens:     true,
	evmUniswapV3ApproveMaxMinusOne:           true,
	evmUniswapV3ApproveZeroThenMaxMinusOne:   true,
	evmUniswapV3RefundETH:                    true,
	evmUniswapV3SweepToken:                   true,
	evmUniswapV3SelfPermitIfNecessary:        true,
	evmUniswapV3ApproveMax:                   true,
	evmUniswapV3ExactInput:                   true,
	evmUniswapV3ExactOutput:                  true,
	evmUniswapV3Factory:                      true,
	evmUniswapV3IncreaseLiquidity:            true,
	evmUniswapV3PositionManager:              true,
	evmUniswapV3WrapETH:                      true,
	evmUniswapV3ExactInputSingle:             true,
	evmUniswapV3FactoryV2:                    true,
	evmUniswapV3GetApprovalType:              true,
	evmUniswapV3UniswapV3SwapCallback:        true,
	evmUniswapV3ExactOutputSingle:            true,
	evmUniswapV3ApproveZeroThenMax:           true,
	evmUniswapV3SelfPermit:                   true,
	evmUniswapV3SelfPermitAllowed:            true,
	evmUniswapV3SweepTokenWithFee:            true,
	evmUniswapV3UnwrapWETH9:                  true,
	evmUniswapV3UnwrapWETH9WithFee:           true,
	evmUniswapV3SwapTokensForExactTokens:     true,
	evmUniswapV3CheckOracleSlippage:          true,
	evmUniswapV3Pull:                         true,
	evmUniswapV3SelfPermitAllowedIfNecessary: true,
}

func evmProcessUniswapV3Tx(
	ctx *evmContext,
	events *[]*db.Event,
	tx *db.EvmTransactionData,
) {
	selector := binary.BigEndian.Uint32(tx.Input[:4])

	const (
		multicallExtended1 uint32 = 0x5ae401dc
		multicallExtended2 uint32 = 0x1f0464d1
	)

	switch selector {
	case evmUniswapV3Multicall, multicallExtended1, multicallExtended2:
		sender := tx.From

		if !evmWalletOwned(ctx, sender) {
			return
		}

		transfers := evmProcessSwap(
			sender, ctx.network,
			tx.Value, tx.InternalTxs, tx.Events,
		)

		event := evmNewEvent(ctx)
		event.App = "uniswap_v3"
		event.Method = "swap"
		event.Type = db.EventTypeSwap
		event.Transfers = transfers

		*events = append(*events, event)
	}
}
