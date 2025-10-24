package parser

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"taxee/cmd/fetcher/evm"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"time"
)

func queryBatchUntilAllSuccessful(
	client *evm.Client,
	network db.Network,
	requests []*evm.RpcRequest,
) []*evm.BatchResult[any] {
	successResult := make([]*evm.BatchResult[any], len(requests))
	errs := make([]int, len(requests))
	for i := range requests {
		errs[i] = i
	}

	const maxRetries = 5
	var result []*evm.BatchResult[any]

	for range maxRetries {
		r := make([]*evm.RpcRequest, len(errs))
		for i, reqIdx := range errs {
			r[i] = requests[reqIdx]
		}

		var err error
		result, err = client.Batch(network, r)
		assert.NoErr(err, "unable to send batch")

		for i, res := range result {
			reqIdx := errs[i]
			switch {
			case res.Error == nil:
				// sucess
				successResult[reqIdx] = res
			default:
				// err
				errs = append(errs, reqIdx)
			}
		}

		errs = errs[len(result):]
		if len(errs) == 0 {
			break
		}

		result = result[:]
	}

	assert.True(len(errs) == 0, "unable to query batch")

	return successResult
}

func evmIdentifyNonProxyContract(bytecode []byte) []uint32 {
	implementations := make([]uint32, 0)
	contractsIds := make(map[uint32]uint32)

	// NOTE: dispatch methods should be in the bytecode as these instructions:
	// 1. PUSH4 ix = 0x63
	// 2.<6. = method selector
	// 6. EQ ix = 0x14
	for i := 0; i < len(bytecode)-5; i += 1 {
		b := bytecode[i]

		// NOTE: PUSH4 ix
		if b == 0x63 {
			selector := binary.BigEndian.Uint32(bytecode[i+1 : i+5])
			i += 5
			eqIx := bytecode[i]

			if eqIx != 20 {
				continue
			}

			// TODO: maybe use single lookup

			if _, ok := evmErc20Selectors[selector]; ok {
				contractsIds[evmErc20ContractId] ^= selector
			}
			if _, ok := evm1inchSelectors[selector]; ok {
				contractsIds[evm1inchV4ContractId] ^= selector
			}
		}
	}

	for expectedContractId, parsedContractId := range contractsIds {
		if expectedContractId == parsedContractId {
			implementations = append(implementations, expectedContractId)
		}
	}

	return implementations
}

func evmIdentifyContracts(
	client *evm.Client,
	network db.Network,
	addressesWithBlocksMap map[string][]uint64,
	context *evmContext,
) {
	////////////////
	// get bytecodes for contracts
	// NOTE: evm contracts are immutable, no need to keep slot
	addresses := slices.Collect(maps.Keys(addressesWithBlocksMap))

	requests := make([]*evm.RpcRequest, len(addresses))
	for i, address := range addresses {
		requests[i] = client.NewRpcRequest(
			"eth_getCode",
			[]string{address, "latest"},
			i,
		)
	}

	bytecodesResult := queryBatchUntilAllSuccessful(client, network, requests)
	bytecodes := make(map[string][]byte)

	type contractId struct {
		block   uint64
		address string
	}

	// NOTE: This needs to be fetched
	proxyContractsMemorySlots := make(map[contractId][32]byte)
	logicContractsAddresses := make([]string, 0)
	proxiesToLogicContractsAddresses := make(map[contractId]string)
	// erc20TokensDecimals := make(map[string]uint8)

	for i, bytecodeResult := range bytecodesResult {
		assert.True(bytecodeResult.Error == nil, "")

		bytecodeEncoded, ok := bytecodeResult.Data.(string)
		assert.True(ok, "invalid bytecode type: %T", bytecodeResult.Data)

		if bytecodeEncoded == "0x" {
			// not a contract
			continue
		}

		bytecode, err := hex.DecodeString(bytecodeEncoded[2:])
		assert.NoErr(err, "invalid bytecode")

		contractAddress := addresses[i]
		bytecodes[contractAddress] = bytecode

		if implMemorySlot, ok := evmIdentifyProxyContract(bytecode); ok {
			blocks := addressesWithBlocksMap[contractAddress]
			for _, b := range blocks {
				proxyContractsMemorySlots[contractId{
					block:   b,
					address: contractAddress,
				}] = implMemorySlot
			}
			continue
		}

		if logicContractAddressBytes, ok := evmIdentifyEip1167ProxyContract(bytecode); ok {
			// TODO: this can only be one, so set block to static value
			logicContractAddress := fmt.Sprintf(
				"0x%s",
				hex.EncodeToString(logicContractAddressBytes[:]),
			)
			proxiesToLogicContractsAddresses[contractId{
				block:   math.MaxUint64,
				address: contractAddress,
			}] = logicContractAddress

			if !slices.Contains(logicContractsAddresses, logicContractAddress) {
				logicContractsAddresses = append(
					logicContractsAddresses,
					logicContractAddress,
				)
			}

			continue
		}

		// TODO: Gnome proxy, ...

		blocks := addressesWithBlocksMap[contractAddress]

		for _, b := range blocks {
			implementations := evmIdentifyNonProxyContract(
				bytecode,
				// contractAddress,
				// erc20TokensDecimals,
			)
			evmAppendContractImplementation(
				context,
				contractAddress,
				b,
				implementations,
			)
		}

	}

	////////////////
	// get logic contracts addreses for proxies

	if len(proxyContractsMemorySlots) > 0 {
		memorySlotsKeys := slices.Collect(maps.Keys(proxyContractsMemorySlots))
		requests = make([]*evm.RpcRequest, len(memorySlotsKeys))

		for i, contractId := range memorySlotsKeys {
			memSlot := proxyContractsMemorySlots[contractId]
			memSlotHex := hex.EncodeToString(memSlot[:])

			requests[i] = client.NewRpcRequest(
				"eth_getStorageAt",
				[]string{
					contractId.address,
					fmt.Sprintf("0x%s", memSlotHex),
					fmt.Sprintf("0x%x", contractId.block),
				},
				i,
			)
		}

		implAddressesResults := queryBatchUntilAllSuccessful(client, network, requests)

		for i, implAddressResult := range implAddressesResults {
			assert.True(implAddressResult.Error == nil, "")

			valueEncoded, ok := implAddressResult.Data.(string)
			assert.True(ok, "invalid address type: %T", implAddressResult.Data)
			assert.True(strings.HasPrefix(valueEncoded, "0x"), "not a valid hex string: %s", valueEncoded)

			// NOTE: the value at the memory slot is 32 bytes long
			// so 64 characters as hex and the last 20 bytes is the address
			// so it's 2 (0x prefix) + 12 * 2 (24) = 26
			address := fmt.Sprintf("0x%s", valueEncoded[26:])
			proxiesToLogicContractsAddresses[memorySlotsKeys[i]] = address

			// NOTE: if bytecode for the implementation contract is already fetched
			// don't need to fetch it again
			if _, ok := bytecodes[address]; !ok {
				// NOTE: only need to fetch once for each address
				if !slices.Contains(logicContractsAddresses, address) {
					logicContractsAddresses = append(logicContractsAddresses, address)
				}
			}
		}
	}

	////////////////
	// get bytecodes of the implementation contracts

	if len(logicContractsAddresses) > 0 {
		requests = make([]*evm.RpcRequest, len(logicContractsAddresses))
		for i, implAddress := range logicContractsAddresses {
			requests[i] = client.NewRpcRequest(
				"eth_getCode",
				[]string{implAddress, "latest"},
				i,
			)
		}

		implBytecodesResult := queryBatchUntilAllSuccessful(client, network, requests)

		for i, bytecodeResult := range implBytecodesResult {
			assert.True(bytecodeResult.Error == nil, "")

			bytecodeEncoded, ok := bytecodeResult.Data.(string)
			assert.True(ok, "invalid bytecode type: %T", bytecodeResult.Data)

			if bytecodeEncoded == "0x" {
				// not a contract
				continue
			}

			bytecode, err := hex.DecodeString(bytecodeEncoded[2:])
			assert.NoErr(err, "invalid bytecode")

			address := logicContractsAddresses[i]
			bytecodes[address] = bytecode
		}
	}

	////////////////
	// parse the proxy contracts

	for proxyContractId, logicContractAddress := range proxiesToLogicContractsAddresses {
		logicContractBytecode := bytecodes[logicContractAddress]
		implementations := evmIdentifyNonProxyContract(
			logicContractBytecode,
			// proxyContractId.address,
			// erc20TokensDecimals,
		)
		evmAppendContractImplementation(
			context,
			proxyContractId.address,
			proxyContractId.block,
			implementations,
		)
	}

	////////////////
	// fetch decimals for erc20 tokens

	// erc20Tokens := slices.Collect(maps.Keys(erc20TokensDecimals))
	// requests = make([]*evm.RpcRequest, len(erc20Tokens))
	//
	// for i, token := range erc20Tokens {
	// 	const decimalsSelector = "0x313ce567"
	// 	requests[i] = client.NewRpcRequest(
	// 		"eth_call",
	// 		[]string{},
	// 		i,
	// 	)
	// 	_ = token
	// }
}

type evmContractImplementation struct {
	block uint64
	impl  []uint32
}

type evmContext struct {
	contracts map[string][]evmContractImplementation
	wallets   []string
	network   db.Network

	// different for each tx
	timestamp time.Time
	txId      string
}

func evmAppendContractImplementation(
	context *evmContext,
	address string,
	blockNumber uint64,
	implementations []uint32,
) {
	c, ok := context.contracts[address]
	if !ok {
		context.contracts[address] = []evmContractImplementation{{
			block: blockNumber,
			impl:  implementations,
		}}
		return
	}

	c = append(c, evmContractImplementation{
		block: blockNumber,
		impl:  implementations,
	})
	context.contracts[address] = c
}

func evmFindContract(
	context *evmContext,
	address string,
	blockNumber uint64,
) (c evmContractImplementation, ok bool) {
	contractsLifetimes, ok := context.contracts[address]
	if !ok {
		return
	}

	for _, l := range contractsLifetimes {
		if l.block == blockNumber {
			c, ok = l, true
			return
		}
	}

	return
}

func evmWalletOwned(ctx *evmContext, address string) bool {
	for _, a := range ctx.wallets {
		if a == address {
			return true
		}
	}
	return false
}

func newEvmEvent(ctx *evmContext) *db.Event {
	return &db.Event{
		Timestamp: ctx.timestamp,
		Network:   ctx.network,
		TxId:      ctx.txId,
	}
}

func evmProcessTx(
	context *evmContext,
	events *[]*db.Event,
	txData *db.EvmTransactionData,
) {
	contract, ok := evmFindContract(context, txData.To, txData.Block)

	if !ok {
		// native transfer
		return
	}

	if slices.Contains(contract.impl, evmErc20ContractId) {
		// erc20 tx
		evmProcessErc20Tx(context, events, txData)
	}
}
