package parser

const (
	evmParaswapWHITELISTED_ROLE       uint32 = 0x7a3226ec // WHITELISTED_ROLE()
	evmParaswapGetImplementation      uint32 = 0xdc9cc645 // getImplementation(bytes4)
	evmParaswapGetPartnerFeeStructure uint32 = 0x6df77496 // getPartnerFeeStructure(address)
	evmParaswapGetRoleMember          uint32 = 0x9010d07c // getRoleMember(bytes32,uint256)
	evmParaswapGrantRole              uint32 = 0x2f2ff15d // grantRole(bytes32,address)
	evmParaswapHasRole                uint32 = 0x91d14854 // hasRole(bytes32,address)
	evmParaswapIsAdapterInitialized   uint32 = 0x3a9243d7 // isAdapterInitialized(bytes32)
	evmParaswapSetImplementation      uint32 = 0x815f6fd  // setImplementation(bytes4,address)
	evmParaswapDEFAULT_ADMIN_ROLE     uint32 = 0xa217fddf // DEFAULT_ADMIN_ROLE()
	evmParaswapGetFeeWallet           uint32 = 0x5459060d // getFeeWallet()
	evmParaswapGetRoleAdmin           uint32 = 0x248a9ca3 // getRoleAdmin(bytes32)
	evmParaswapGetRouterData          uint32 = 0xaa97ef02 // getRouterData(bytes32)
	evmParaswapInitializeRouter       uint32 = 0x60e35507 // initializeRouter(address,bytes)
	evmParaswapRegisterPartner        uint32 = 0xaa5b2458 // registerPartner(address,uint256,bool,bool,uint16,string,bytes)
	evmParaswapGetAdapterData         uint32 = 0x9a5a98d3 // getAdapterData(bytes32)
	evmParaswapGetRoleMemberCount     uint32 = 0xca15c873 // getRoleMemberCount(bytes32)
	evmParaswapGetTokenTransferProxy  uint32 = 0xd2c4b598 // getTokenTransferProxy()
	evmParaswapGetVersion             uint32 = 0xd8e6e2c  // getVersion()
	evmParaswapRenounceRole           uint32 = 0x36568abe // renounceRole(bytes32,address)
	evmParaswapSetFeeWallet           uint32 = 0x90d49b9d // setFeeWallet(address)
	evmParaswapROUTER_ROLE            uint32 = 0x30d643b5 // ROUTER_ROLE()
	evmParaswapInitializeAdapter      uint32 = 0x18800219 // initializeAdapter(address,bytes)
	evmParaswapIsRouterInitialized    uint32 = 0x9812f33b // isRouterInitialized(bytes32)
	evmParaswapRevokeRole             uint32 = 0xd547741f // revokeRole(bytes32,address)
	evmParaswapTransferTokens         uint32 = 0xa64b6e5f // transferTokens(address,address,uint256)

	evmParaswapContractId uint32 = 0x8f1483ac
)

var evmParaswapSelectors = map[uint32]bool{
	evmParaswapWHITELISTED_ROLE:       true,
	evmParaswapGetImplementation:      true,
	evmParaswapGetPartnerFeeStructure: true,
	evmParaswapGetRoleMember:          true,
	evmParaswapGrantRole:              true,
	evmParaswapHasRole:                true,
	evmParaswapIsAdapterInitialized:   true,
	evmParaswapSetImplementation:      true,
	evmParaswapDEFAULT_ADMIN_ROLE:     true,
	evmParaswapGetFeeWallet:           true,
	evmParaswapGetRoleAdmin:           true,
	evmParaswapGetRouterData:          true,
	evmParaswapInitializeRouter:       true,
	evmParaswapRegisterPartner:        true,
	evmParaswapGetAdapterData:         true,
	evmParaswapGetRoleMemberCount:     true,
	evmParaswapGetTokenTransferProxy:  true,
	evmParaswapGetVersion:             true,
	evmParaswapRenounceRole:           true,
	evmParaswapSetFeeWallet:           true,
	evmParaswapROUTER_ROLE:            true,
	evmParaswapInitializeAdapter:      true,
	evmParaswapIsRouterInitialized:    true,
	evmParaswapRevokeRole:             true,
	evmParaswapTransferTokens:         true,
}
