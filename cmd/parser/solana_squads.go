package parser

import (
	"encoding/binary"
	"fmt"
	"taxee/pkg/assert"
	"taxee/pkg/db"

	"github.com/mr-tron/base58"
)

const SOL_SQUADS_V4_PROGRAM_ADDRESS = "SQDS4ep65T869zMMBKyuUq6aD6EgTu8psMjkvj52pCf"

type solSquadsIx uint8

const (
	solSquadsV4ConfigTransactionCreate solSquadsIx = iota

	solSquadsV4VaultTransactionCreate
	solSquadsV4VaultTransactionExecute
)

func solSquadsV4IxFromData(data []byte) (ix solSquadsIx, method string, ok bool) {
	assert.True(len(data) >= 8, "invalid data: %#v", data)
	ok = true

	var disc [8]byte
	copy(disc[:], data[:8])

	switch disc {
	case [8]uint8{48, 250, 78, 168, 208, 226, 218, 211}:
		ix, method = solSquadsV4VaultTransactionCreate, "vault_transaction_create"
	case [8]uint8{194, 8, 161, 87, 153, 164, 25, 171}:
		ix, method = solSquadsV4VaultTransactionExecute, "vault_transaction_execute"
	default:
		ok = false
	}

	return
}

func solSquadsV4IxRelatedAccounts(
	relatedAccounts relatedAccounts,
	walletAddress string,
	ix *db.SolanaInstruction,
) {
	ixType, _, ok := solSquadsV4IxFromData(ix.Data)
	if !ok {
		return
	}

	_ = walletAddress

	switch ixType {
	case solSquadsV4VaultTransactionCreate:
		transaction := ix.Accounts[1]
		relatedAccounts.Append(transaction)
	case solSquadsV4VaultTransactionExecute:
		transaction := ix.Accounts[2]
		relatedAccounts.Append(transaction)
	}
}

type solSquadsV4Lookup struct {
	writableCount uint8
	readonlyCount uint8
}

type solSquadsV4Ix struct {
	programIdx   uint8
	accountsIdxs []uint8
	data         []byte
}

type solSquadsV4CompiledTxAccountData struct {
	accountKeys []string
	ixs         []solSquadsV4Ix
	lookups     []solSquadsV4Lookup
}

func (_ solSquadsV4CompiledTxAccountData) name() string {
	return "squadsV4_comp_transaction_account"
}

type solSquadsV4DecompiledTxAccountData []*db.SolanaInstruction

func (_ solSquadsV4DecompiledTxAccountData) name() string {
	return "squadsV4_decomp_transaction_account"
}

func (acc *solSquadsV4CompiledTxAccountData) initFromData(data []byte) {
	// https://github.com/Squads-Protocol/v4/blob/main/programs/squads_multisig_program/src/instructions/vault_transaction_create.rs
	// disc => 8 bytes
	// vault_index => 1 byte
	// ephemeral signers => 1 byte
	// transaction messages byte vec size => 4 bytes (u32)
	msg := data[14:]

	accountKeysCount := msg[3]
	acc.accountKeys = make([]string, accountKeysCount)
	for i := range accountKeysCount {
		offset := 4 + i*32
		acc.accountKeys[i] = base58.Encode(msg[offset : offset+32])
	}

	ixsCountOffset := 4 + accountKeysCount*32
	ixsCount := msg[ixsCountOffset]

	acc.ixs = make([]solSquadsV4Ix, ixsCount)
	offset := int(ixsCountOffset) + 1
	for i := range ixsCount {
		programIdx := msg[offset]
		offset += 1

		accountsIdxsCount := int(msg[offset])
		offset += 1

		accountsIdxs := msg[offset : offset+accountsIdxsCount]
		offset += accountsIdxsCount

		dataBytesCount := int(binary.LittleEndian.Uint16(
			msg[offset : offset+2],
		))
		offset += 2
		dataBytes := msg[offset : offset+dataBytesCount]
		offset += dataBytesCount

		ix := solSquadsV4Ix{
			programIdx:   programIdx,
			accountsIdxs: accountsIdxs,
			data:         dataBytes,
		}
		acc.ixs[i] = ix
	}

	lookupsCount := msg[offset]
	offset += 1

	acc.lookups = make([]solSquadsV4Lookup, lookupsCount)

	for i := range lookupsCount {
		// addres => 32
		offset += 32

		writableCount := msg[offset]
		offset += 1 + int(writableCount)

		readonlyCount := msg[offset]
		offset += 1 + int(readonlyCount)

		lookup := solSquadsV4Lookup{
			writableCount, readonlyCount,
		}
		acc.lookups[i] = lookup
	}
}

// implemented based on:
// https://github.com/Squads-Protocol/v4/blob/main/programs/squads_multisig_program
func solPreprocessSquadsV4Ix(ctx *solContext, ix *db.SolanaInstruction) {
	ixType, _, ok := solSquadsV4IxFromData(ix.Data)
	if !ok {
		return
	}

	switch ixType {
	case solSquadsV4VaultTransactionCreate:
		transaction, owner := ix.Accounts[1], ix.Accounts[2]
		owned := ctx.walletOwned(owner)

		var accountData solSquadsV4CompiledTxAccountData
		accountData.initFromData(ix.Data)

		innerIxsIter := solInnerIxIterator{innerIxs: ix.InnerInstructions}
		_, to, amount, err := solAnchorInitAccountValidate(&innerIxsIter)
		assert.NoErr(err, "")

		ctx.receiveSol(to, amount)
		ctx.init(transaction, owned, &accountData)
	case solSquadsV4VaultTransactionExecute:
		transaction := ix.Accounts[2]
		transactionAccount := ctx.find(ctx.slot, ctx.ixIdx, transaction, 2)
		assert.True(
			transactionAccount != nil,
			"missing squads tx account: %s %s",
			transaction, ctx.txId,
		)

		squadsTxAccountData, ok := solAccountDataMust[solSquadsV4CompiledTxAccountData](
			ctx,
			transactionAccount,
			transaction,
		)
		if !ok {
			return
		}

		// `remaining_accounts` must include the following accounts in the exact order:
		// 1. AddressLookupTable accounts in the order they appear in `message.address_table_lookups`.
		// 2. Accounts in the order they appear in `message.account_keys`.
		// 3. Accounts in the order they appear in `message.address_table_lookups`.
		remainingAccounts := ix.Accounts[4:]
		offset := len(squadsTxAccountData.lookups) + len(squadsTxAccountData.accountKeys)

		var writableAccounts, readonlyAccounts []string
		for _, lookup := range squadsTxAccountData.lookups {
			readonlyStart := offset + int(lookup.writableCount)
			nextOffset := readonlyStart + int(lookup.readonlyCount)

			writableAccounts = append(
				writableAccounts,
				remainingAccounts[offset:readonlyStart]...,
			)
			readonlyAccounts = append(
				readonlyAccounts,
				remainingAccounts[readonlyStart:nextOffset]...,
			)

			offset = nextOffset
		}

		squadsTxAccountData.accountKeys = append(
			squadsTxAccountData.accountKeys,
			writableAccounts...,
		)
		squadsTxAccountData.accountKeys = append(
			squadsTxAccountData.accountKeys,
			readonlyAccounts...,
		)

		ixsEq := func(
			ix1, ix2 *db.SolanaInnerInstruction,
		) bool {
			if ix1.ProgramAddress != ix2.ProgramAddress {
				return false
			}
			if len(ix1.Accounts) != len(ix2.Accounts) {
				return false
			}
			if len(ix1.Data) != len(ix2.Data) {
				return false
			}
			for i, a1 := range ix1.Accounts {
				if ix2.Accounts[i] != a1 {
					return false
				}
			}
			for i, d1 := range ix1.Data {
				if ix2.Data[i] != d1 {
					return false
				}
			}
			return true
		}

		decompileIx := func(
			squadsAccount *solSquadsV4CompiledTxAccountData,
			idx int,
		) *db.SolanaInnerInstruction {
			compiledIx := squadsAccount.ixs[idx]

			programAddress := squadsAccount.accountKeys[compiledIx.programIdx]
			accounts := make([]string, len(compiledIx.accountsIdxs))
			for i, accIdx := range compiledIx.accountsIdxs {
				accounts[i] = squadsAccount.accountKeys[accIdx]
			}

			return &db.SolanaInnerInstruction{
				ProgramAddress: programAddress,
				Accounts:       accounts,
				Data:           compiledIx.data,
			}
		}

		squadsExecutedIxs := make([]*db.SolanaInstruction, len(squadsTxAccountData.ixs))
		// NOTE: There should always be at least one inner instruction
		firstSquadsIx := decompileIx(squadsTxAccountData, 0)
		firstSquadsIx.Logs = ix.InnerInstructions[0].Logs
		firstSquadsIx.ReturnData = ix.InnerInstructions[0].ReturnData
		squadsExecutedIxs[0] = &db.SolanaInstruction{
			SolanaInnerInstruction: decompileIx(squadsTxAccountData, 0),
		}
		runningInnerIxIdx := 1

		for i := 1; i < len(squadsTxAccountData.ixs); i += 1 {
			squadsIx := decompileIx(squadsTxAccountData, i)
			prevSquadsIx := squadsExecutedIxs[i-1]

			// NOTE: accumulate inner instructions to previous squads ix
			for ; runningInnerIxIdx < len(ix.InnerInstructions); runningInnerIxIdx += 1 {
				realIx := ix.InnerInstructions[runningInnerIxIdx]
				if ixsEq(squadsIx, realIx) {
					squadsIx.Logs = realIx.Logs
					squadsIx.ReturnData = realIx.ReturnData
					squadsExecutedIxs[i] = &db.SolanaInstruction{
						SolanaInnerInstruction: squadsIx,
					}
					// NOTE: skip over current one, since it's the squads ix
					runningInnerIxIdx += 1
					break
				}

				prevSquadsIx.InnerInstructions = append(
					prevSquadsIx.InnerInstructions,
					realIx,
				)
			}
		}

		transactionAccount.Data = (*solSquadsV4DecompiledTxAccountData)(
			&squadsExecutedIxs,
		)

		for _, squadsIx := range squadsExecutedIxs {
			solPreprocessIx(ctx, squadsIx)
		}
	}
}

func solProcessSquadsV4Ix(
	ctx *solContext,
	ix *db.SolanaInstruction,
	events *[]*db.Event,
) {
	ixType, method, ok := solSquadsV4IxFromData(ix.Data)
	if !ok {
		return
	}

	const app = "squads"

	switch ixType {
	case solSquadsV4VaultTransactionCreate:
		innerIxs := solInnerIxIterator{innerIxs: ix.InnerInstructions}
		event, ok, err := solProcessAnchorInitAccount(ctx, &innerIxs)
		assert.NoErr(err, fmt.Sprintf("unable to process ix: %s", ctx.txId))
		if !ok {
			return
		}

		event.App = app
		event.Method = method

		*events = append(*events, event)
	case solSquadsV4VaultTransactionExecute:
		transaction := ix.Accounts[2]
		transactionAccount := ctx.find(ctx.slot, ctx.ixIdx, transaction, 2)
		assert.True(
			transactionAccount != nil,
			"missing squads tx account: %s %s",
			transaction, ctx.txId,
		)

		squadsInstructions, ok := solAccountDataMust[solSquadsV4DecompiledTxAccountData](
			ctx,
			transactionAccount,
			transaction,
		)
		if !ok {
			return
		}

		for _, squadsIx := range *squadsInstructions {
			solProcessIx(events, ctx, squadsIx)
		}
	}
}
