package parser

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"slices"
	"strings"
	"taxee/cmd/fetcher/evm"
	"taxee/pkg/db"
	"taxee/pkg/jsonrpc"
	"time"

	"github.com/shopspring/decimal"
)

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

// Identifies contracts in 4 steps
//
// - Fetch bytecodes for all addresses
//
// - Detect proxies (slot-based or EIP-1167)
//
// - Fetch implementation bytecodes
//
// - Analyze implementations and associate with proxy addresses
func evmIdentifyContracts(
	ctx context.Context,
	network db.Network,
	alchemyApiKey string,
	addressesWithBlocksMap map[string][]uint64,
	context *evmContext,
) error {
	url, err := evm.AlchemyApiUrl(network, alchemyApiKey)
	if err != nil {
		return err
	}

	bytecodes := make(map[string][]byte)

	type contractId struct {
		address     string
		blockNumber uint64
	}

	type storageSlotRequest struct {
		proxyAddress string
		blockNumber  uint64
		slot         [32]byte
	}

	proxies := make(map[contractId]string)
	var storageSlotsToGet []*storageSlotRequest
	implAddressesToGet := make(map[string]bool)

	rpcBatch := jsonrpc.NewBatch(url)

	for address, blocks := range addressesWithBlocksMap {
		rpcBatch.Queue(
			evm.GetCode, &evm.GetCodeParams{
				Address:  address,
				BlockTag: evm.BlockTagLatest,
			},
			func(rm json.RawMessage) (retry bool, err error) {
				if string(rm) == "null" {
					return true, nil
				}

				var code evm.DataBytes
				if err := json.Unmarshal(rm, &code); err != nil {
					return false, fmt.Errorf("unable to unmarshal code: %w", err)
				}

				if len(code) == 0 {
					// not a contract
					return false, nil
				}

				// process bytecode

				// identify proxy contract based on implementation memory slot
				// need to fetch the implementation slot at all blocks occurences
				if implMemorySlot, ok := evmIdentifyProxyContract(code); ok {
					for _, b := range blocks {
						storageSlotsToGet = append(storageSlotsToGet, &storageSlotRequest{
							proxyAddress: address,
							blockNumber:  b,
							slot:         implMemorySlot,
						})
					}
					return false, nil
				}

				// identify proxy contract based on EIP1167, returns
				// implementation address
				if implAddressBytes, ok := evmIdentifyEip1167ProxyContract(code); ok {
					implAddress := fmt.Sprintf("0x%s", hex.EncodeToString(implAddressBytes[:]))
					implAddressesToGet[implAddress] = true

					for _, b := range blocks {
						proxies[contractId{address, b}] = implAddress
					}

					return false, nil
				}

				// TODO: Gnome proxy, ...

				// not a proxy contract

				bytecodes[address] = code

				implementations := evmIdentifyNonProxyContract(code)
				for _, b := range addressesWithBlocksMap[address] {
					context.appendContractImplementation(
						address, b, implementations,
					)
				}

				return false, nil
			},
		)
	}

	if err := jsonrpc.CallBatch(ctx, rpcBatch); err != nil {
		return fmt.Errorf("unable to get bytecodes: %w", err)
	}

	rpcBatch = jsonrpc.NewBatch(url)

	for _, r := range storageSlotsToGet {
		rpcBatch.Queue(
			evm.GetStorageAt, &evm.GetStorageAtParams{
				Address:     r.proxyAddress,
				StorageSlot: r.slot[:],
				BlockNumber: evm.QuantityUint64(r.blockNumber),
			},
			func(rm json.RawMessage) (retry bool, err error) {
				if string(rm) == "null" {
					return true, nil
				}

				// NOTE: this should always be non empty value with size of 32
				// bytes
				var result evm.DataBytes32
				if err := json.Unmarshal(rm, &result); err != nil {
					return false, fmt.Errorf("unable to unmarshal storage slot: %w", err)
				}

				// assign implementations to proxies

				// address is last 20 bytes
				implAddress := fmt.Sprintf("0x%s", hex.EncodeToString(result[12:]))
				if _, ok := bytecodes[implAddress]; !ok {
					implAddressesToGet[implAddress] = true
				}

				proxies[contractId{r.proxyAddress, r.blockNumber}] = implAddress
				return false, nil
			},
		)
	}

	if err := jsonrpc.CallBatch(ctx, rpcBatch); err != nil {
		return fmt.Errorf("unable to get implementation addresses: %w", err)
	}

	rpcBatch = jsonrpc.NewBatch(url)

	for implAddress := range implAddressesToGet {
		p := evm.GetCodeParams{
			Address:  implAddress,
			BlockTag: evm.BlockTagLatest,
		}
		rpcBatch.Queue(evm.GetCode, &p, func(rm json.RawMessage) (retry bool, err error) {
			if string(rm) == "null" {
				return true, nil
			}

			var code evm.DataBytes
			if err := json.Unmarshal(rm, &code); err != nil {
				return false, fmt.Errorf("unable to unmarshal code: %w", err)
			}

			if len(code) == 0 {
				// NOTE: this is a proxy implementation fetched at block when
				// the contract was called, it has to exist
				//
				// TODO: I guess calling selfdestruct could remove the code, so maybe
				// the code should be fetched at the block it was called
				return false, fmt.Errorf("should be a contract: %s", implAddress)
			}

			bytecodes[implAddress] = code
			return false, nil
		})
	}

	if err := jsonrpc.CallBatch(ctx, rpcBatch); err != nil {
		return fmt.Errorf("unable to get bytecodes: %w", err)
	}

	// go over all the proxies

	for proxy, implementationAddress := range proxies {
		impls := evmIdentifyNonProxyContract(bytecodes[implementationAddress])
		context.appendContractImplementation(
			proxy.address, uint64(proxy.blockNumber), impls,
		)
	}

	return nil
}

type evmContractImplementation struct {
	block uint64
	impl  []uint32
}

type evmContext struct {
	contracts map[string][]evmContractImplementation
	wallets   []string
	network   db.Network
	decimals  map[string]uint8

	// different for each tx
	timestamp time.Time
	txId      string
}

func (c *evmContext) appendContractImplementation(
	address string,
	blockNumber uint64,
	implementations []uint32,
) {
	c.contracts[address] = append(c.contracts[address], evmContractImplementation{
		block: blockNumber,
		impl:  implementations,
	})
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

func evmProcessSwap(
	sender string,
	network db.Network,
	value decimal.Decimal,
	itxs []*db.EvmInternalTx,
	logs []*db.EvmTransactionEvent,
) []*db.EventTransfer {
	amounts := make(map[string]decimal.Decimal)

	if !value.Equal(decimal.Zero) {
		amounts["ethereum"] = value.Neg()
	}

	for _, itx := range itxs {
		switch sender {
		case itx.From:
			amounts["ethereum"] = amounts["ethereum"].Sub(itx.Value)
		case itx.To:
			amounts["ethereum"] = amounts["ethereum"].Add(itx.Value)
		}
	}

	for _, log := range logs {
		if slices.Equal(log.Topics[0], evmErc20TransferTopic[:]) {
			from := evmAddressFrom32Bytes(log.Topics[1])
			to := evmAddressFrom32Bytes(log.Topics[2])
			amount := evmAmountFrom32Bytes(log.Data[:32])

			switch sender {
			case from:
				amounts[log.Address] = amounts[log.Address].Sub(amount)
			case to:
				amounts[log.Address] = amounts[log.Address].Add(amount)
			}
		}
	}

	var transfers []*db.EventTransfer

	for token, amount := range amounts {
		if amount.Equal(decimal.Zero) {
			continue
		}

		t := db.EventTransfer{
			Token:       token,
			Amount:      amount.Abs(),
			TokenSource: uint16(network),
		}
		if token == "ethereum" {
			t.TokenSource = math.MaxUint16
		}

		if amount.LessThan(decimal.Zero) {
			t.Direction = db.EventTransferOutgoing
			t.FromAccount = sender
			t.FromWallet = sender
			transfers = append(transfers, &t)
		} else if amount.GreaterThan(decimal.Zero) {
			t.Direction = db.EventTransferIncoming
			t.ToAccount = sender
			t.ToWallet = sender
			transfers = append(transfers, &t)
		}
	}

	// sort so we have the same ordering each time
	// outgoing first, incoming second and then by token
	slices.SortFunc(transfers, func(a *db.EventTransfer, b *db.EventTransfer) int {
		adir, bdir := a.Direction, b.Direction
		if adir == bdir {
			return strings.Compare(a.Token, b.Token)
		}
		return adir.Cmp(bdir)
	})

	return transfers
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

	transfers := evmProcessSwap(
		sender, ctx.network,
		tx.Value, tx.InternalTxs, tx.Events,
	)

	if len(transfers) == 0 {
		return
	}

	event := evmNewEvent(ctx)
	event.App = appName
	event.Method = methodName
	event.Type = db.EventTypeSwap
	event.Transfers = transfers

	*events = append(*events, event)
}

func evmProcessTx(
	ctx *evmContext,
	events *[]*db.Event,
	txData *db.EvmTransactionData,
) {
	defer func() {
		if cause := recover(); cause != nil {
			slog.Error(
				"evm process ix panicked",
				"txId", ctx.txId,
				"cause", cause,
				"stack", string(debug.Stack()),
			)
		}
	}()

	contract, ok := evmFindContract(ctx, txData.To, txData.Block)

	if !ok && txData.Value.GreaterThan(decimal.Zero) {
		fromInternal := evmWalletOwned(ctx, txData.From)
		toInternal := evmWalletOwned(ctx, txData.To)

		if !fromInternal && !toInternal {
			return
		}

		event := evmNewEvent(ctx)
		event.App = "native"
		event.Method = "transfer"
		event.Type = db.EventTypeTransfer
		event.Transfers = append(event.Transfers, &db.EventTransfer{
			Direction:   getTransferEventDirection(fromInternal, toInternal),
			FromWallet:  txData.From,
			ToWallet:    txData.To,
			FromAccount: txData.From,
			ToAccount:   txData.To,
			Token:       "ethereum",
			Amount:      txData.Value,
			TokenSource: tokenSourceCoingecko,
		})
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
				evmProcessSwapTx(ctx, events, txData, "1inch", "swap")
			case evm1inchVUnknownContractId:
				evmProcessSwapTx(ctx, events, txData, "1inch", "swap")
			case evmUniswapV3ContractId:
				evmProcessUniswapV3Tx(ctx, events, txData)
			case evmTraderJoeRouter2ContractId:
				evmProcessSwapTx(ctx, events, txData, "trader_joe", "swap")
			case evmParaswapContractId:
				evmProcessSwapTx(ctx, events, txData, "paraswap", "swap")
			}
		}
	}
}
