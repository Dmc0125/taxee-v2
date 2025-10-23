package parser

// implementation based on: https://eips.ethereum.org/EIPS/eip-1167
func evmIdentifyEip1167ProxyContract(bytecode []byte) (address [20]byte, ok bool) {
	if len(bytecode) < 10 {
		return
	}
	// the 10th byte can be any of pushn ix where n <= 20
	// and that also defines the length of the address
	preAddressBytes := [9]uint8{54, 61, 61, 55, 61, 61, 61, 54, 61}

	for i := range 9 {
		if bytecode[i] != preAddressBytes[i] {
			return
		}
	}

	// push1 => 96
	// push20 => 115
	pushIx := bytecode[9]
	if pushIx < 96 || pushIx > 115 {
		return
	}

	addressLen := int(pushIx - 95)
	postAddressPos := 10 + addressLen
	if len(bytecode) < postAddressPos {
		return
	}
	copy(address[20-addressLen:], bytecode[10:postAddressPos])

	postAddressBytes := [15]uint8{90, 244, 61, 130, 128, 62, 144, 61, 145, 96, 43, 87, 253, 91, 243}
	for i := range 15 {
		b := bytecode[i+postAddressPos]
		if b != postAddressBytes[i] {
			return
		}
	}

	ok = true
	return
}

var (
	evmEip1967ImplementationMemoryAddress         = [32]byte{54, 8, 148, 161, 59, 161, 163, 33, 6, 103, 200, 40, 73, 45, 185, 141, 202, 62, 32, 118, 204, 55, 53, 169, 32, 163, 202, 80, 93, 56, 43, 188}
	evmOldOpenZeppelinImplementationMemoryAddress = [32]byte{112, 80, 201, 224, 244, 202, 118, 156, 105, 189, 58, 142, 247, 64, 188, 55, 147, 79, 142, 44, 3, 110, 90, 114, 63, 216, 238, 4, 142, 211, 248, 195}
)

// identifies the contract as a proxy based on push32 ix followed by
// implementation memory address:
//
// 1. 0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc
//   - https://eips.ethereum.org/EIPS/eip-1967
//
// 2. 0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3
//   - old open zeppelin
//   - USDC on arbitrum: https://arbiscan.io/token/0xaf88d065e77c8cc2239327c5edb3a432268e5831#code
//
// afaik, the contract bytecode has to contain these hashes if it is a proxy
// implementation, however depending on how the code is written, it may be
// compiled differently though there should not be a reason for it, so it is
// a really niche case
//
// based on claude: the push32 may be split to 2 push16 instructions and vice versa
// and then this function fails
func evmIdentifyProxyContract(
	bytecode []byte,
) (memoryAddress [32]byte, ok bool) {
	if len(bytecode) < 32 {
		return
	}

	for i := 0; i < len(bytecode); i += 1 {
		b := bytecode[i]

		// push32 ix
		if b == 127 {
			address := [32]byte{}
			if len(bytecode[i+1:]) < 32 {
				// NOTE: ignore, most likely because we are in the hash alerady
				return
			}
			copy(address[:], bytecode[i+1:i+33])

			switch address {
			case evmEip1967ImplementationMemoryAddress:
				memoryAddress = evmEip1967ImplementationMemoryAddress
				ok = true
				return
			case evmOldOpenZeppelinImplementationMemoryAddress:
				memoryAddress = evmOldOpenZeppelinImplementationMemoryAddress
				ok = true
				return
			}

			i += 32
		}
	}

	return
}
