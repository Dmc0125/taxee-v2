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

	"github.com/shopspring/decimal"
)

func queryBatchUntilAllSuccessful(
	client *evm.Client,
	network db.Network,
	requests []*evm.RpcRequest,
) []*evm.BatchResult[any] {
	type queuedRequest struct {
		idx    int
		errors []*evm.RpcError
	}

	queue := make(map[int]*queuedRequest)
	for i, req := range requests {
		queue[i] = &queuedRequest{
			idx: req.Id,
		}
	}

	batchResult := make([]*evm.BatchResult[any], len(requests))

	for try := 0; len(queue) > 0 && try < 5; try += 1 {
		r, reqIdx := make([]*evm.RpcRequest, len(queue)), 0
		for _, qr := range queue {
			r[reqIdx] = requests[qr.idx]
			reqIdx += 1
		}

		result, err := client.Batch(network, r)
		assert.NoErr(err, "unable to send batch")

		for i, res := range result {
			req := *r[i]
			qr := queue[req.Id]

			switch {
			case res.Error == nil:
				batchResult[qr.idx] = res
				delete(queue, req.Id)
			default:
				qr.errors = append(qr.errors, res.Error)
			}
		}
	}

	if len(queue) != 0 {
		for _, qr := range queue {
			req := requests[qr.idx]
			fmt.Printf("Method: %s\n", req.Method)
			fmt.Printf("Params: %#v\n", req.Params)
			for i, err := range qr.errors {
				fmt.Printf("\t%d: Err: %#v\n", i, err)
			}
		}
		fmt.Println()
		assert.True(false, "unable to query batch")
	}

	return batchResult
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
			eqIx := bytecode[i+5]

			if eqIx != 20 {
				continue
			}

			// NOTE: since we are not parsing all the EVM instruction, we skip
			// only if we successfully find the method dispatcher, otherwise
			// there can be false positives
			i += 5

			// TODO: maybe use single lookup

			if _, ok := evmErc20Selectors[selector]; ok {
				contractsIds[evmErc20ContractId] ^= selector
			}
			if _, ok := evm1inchV4Selectors[selector]; ok {
				contractsIds[evm1inchV4ContractId] ^= selector
			}
			if _, ok := evm1inchVUnknownSelectors[selector]; ok {
				contractsIds[evm1inchVUnknownContractId] ^= selector
			}
			if _, ok := evmUniswapV3Selectors[selector]; ok {
				contractsIds[evmUniswapV3ContractId] ^= selector
			}
			if _, ok := evmTraderJoeRouter2Selectors[selector]; ok {
				contractsIds[evmTraderJoeRouter2ContractId] ^= selector
			}
			if _, ok := evmParaswapSelectors[selector]; ok {
				contractsIds[evmParaswapContractId] ^= selector
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
		implementations := evmIdentifyNonProxyContract(
			bytecode,
		)

		for _, b := range blocks {
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
		)
		evmAppendContractImplementation(
			context,
			proxyContractId.address,
			proxyContractId.block,
			implementations,
		)
	}
}

type evmContractImplementation struct {
	block uint64
	impl  []uint32
}

type evmContext struct {
	contracts     map[string][]evmContractImplementation
	wallets       []string
	network       db.Network
	decimals      map[string]uint8
	processErrors *errorsContainer

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
	return slices.Contains(ctx.wallets, address)
}

func evmNewEvent(ctx *evmContext) *db.Event {
	return &db.Event{
		Timestamp: ctx.timestamp,
		Network:   ctx.network,
		TxId:      ctx.txId,
	}
}

func evmProcessSwap(
	sender string,
	network db.Network,
	value decimal.Decimal,
	itxs []*db.EvmInternalTx,
	logs []*db.EvmTransactionEvent,
) db.EventSwap {
	amounts := make(map[string]decimal.Decimal)

	if !value.Equal(decimal.Zero) {
		amounts["ethereum"] = value.Neg()
	}

	for _, itx := range itxs {
		if sender == itx.From {
			amounts["ethereum"] = amounts["ethereum"].Sub(itx.Value)
		} else if sender == itx.To {
			amounts["ethereum"] = amounts["ethereum"].Add(itx.Value)
		}
	}

	for _, log := range logs {
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
			TokenSource: uint16(network),
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

	return swapData
}

func evmProcessSwapTx(
	ctx *evmContext,
	events *[]*db.Event,
	tx *db.EvmTransactionData,
	appName, methodName string,
) {
	sender := tx.From
	if !evmWalletOwned(ctx, sender) {
		return
	}

	swapData := evmProcessSwap(
		sender, ctx.network,
		tx.Value, tx.InternalTxs, tx.Events,
	)

	if len(swapData.Incoming) == 0 && len(swapData.Outgoing) == 0 {
		return
	}

	event := evmNewEvent(ctx)
	event.UiAppName = appName
	event.UiMethodName = methodName
	event.Type = db.EventTypeSwap
	event.Data = &swapData

	*events = append(*events, event)
}

func evmProcessTx(
	ctx *evmContext,
	events *[]*db.Event,
	txData *db.EvmTransactionData,
) {
	contract, ok := evmFindContract(ctx, txData.To, txData.Block)

	if !ok && txData.Value.GreaterThan(decimal.Zero) {
		fromInternal := evmWalletOwned(ctx, txData.From)
		toInternal := evmWalletOwned(ctx, txData.To)

		if !fromInternal && !toInternal {
			return
		}

		event := evmNewEvent(ctx)
		event.UiAppName = "native"
		event.UiMethodName = "transfer"

		setEventTransfer(
			event,
			txData.From, txData.To,
			txData.From, txData.To,
			fromInternal, toInternal,
			txData.Value,
			"ethereum",
			tokenSourceCoingecko,
		)

		*events = append(*events, event)
		return
	}

	switch txData.To {
	case evmArbitrumDistributorContractAddress:
		evmProcessArbitrumDistributorTx(ctx, events, txData)
	default:
		for _, contractId := range contract.impl {
			switch contractId {
			case evmErc20ContractId:
				evmProcessErc20Tx(ctx, events, txData)
			case evm1inchV4ContractId:
				evmProcessSwapTx(ctx, events, txData, "paraswap", "swap")
			case evm1inchVUnknownContractId:
				evmProcessSwapTx(ctx, events, txData, "paraswap", "swap")
			case evmUniswapV3ContractId:
				evmProcessUniswapV3Tx(ctx, events, txData)
			case evmTraderJoeRouter2ContractId:
				evmProcessSwapTx(ctx, events, txData, "paraswap", "swap")
			case evmParaswapContractId:
				evmProcessSwapTx(ctx, events, txData, "paraswap", "swap")
			}
		}
	}
}
