package parser

const (
	evm1inchV4LIMIT_ORDER_RFQ_TYPEHASH  uint32 = 0x06bf53d0
	evm1inchV4FillOrderRFQ              uint32 = 0xa5b7c8d4
	evm1inchV4FillOrderRFQToWithPermit  uint32 = 0x6444c2fc
	evm1inchV4Destroy                   uint32 = 0x83197ef0
	evm1inchV4FillOrderRFQTo            uint32 = 0x219408f0
	evm1inchV4InvalidatorForOrderRFQ    uint32 = 0x8298b8e7
	evm1inchV4UnoswapWithPermit         uint32 = 0xdb35c845
	evm1inchV4DOMAIN_SEPARATOR          uint32 = 0x3644e515
	evm1inchV4Owner                     uint32 = 0x8da5cb5b
	evm1inchV4RescueFunds               uint32 = 0xa05c944e
	evm1inchV4Swap                      uint32 = 0x3b962f8a
	evm1inchV4UniswapV3Swap             uint32 = 0x30514862
	evm1inchV4UniswapV3SwapCallback     uint32 = 0xda80644f
	evm1inchV4CancelOrderRFQ            uint32 = 0xfdd062b8
	evm1inchV4RenounceOwnership         uint32 = 0x715018a6
	evm1inchV4TransferOwnership         uint32 = 0x6d08b64f
	evm1inchV4UniswapV3SwapTo           uint32 = 0xd1850379
	evm1inchV4UniswapV3SwapToWithPermit uint32 = 0xb542dfc3
	evm1inchV4Unoswap                   uint32 = 0x81851f09

	evm1inchV4ContractId uint32 = 0xf25ce91f
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
