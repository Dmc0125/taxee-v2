package parser

const (
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

	evm1inchV4ContractId uint32 = 0x4365f2fa
)

var evm1inchSelectors = map[uint32]bool{
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
