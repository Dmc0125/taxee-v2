package solana

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"taxee/pkg/assert"
	"time"

	"github.com/mr-tron/base58/base58"
)

// Decodes an unsigned 16 bit integer one-to-one encoded as follows:
// 1 byte  : 0xxxxxxx                   => 00000000 0xxxxxxx :      0 -    127
// 2 bytes : 1xxxxxxx 0yyyyyyy          => 00yyyyyy yxxxxxxx :    128 - 16,383
// 3 bytes : 1xxxxxxx 1yyyyyyy 000000zz => zzyyyyyy yxxxxxxx : 16,384 - 65,535
func parseCompactUint16(data []byte) (uint16, int, error) {
	const (
		hasNextMask = 0b10000000
		valMask     = 0b01111111
	)

	if len(data) == 0 {
		return 0, 0, errors.New("empty data")
	}

	value := uint16(0)
	i := 0

	for ; i < 3; i += 1 {
		b := data[i]

		value += uint16((b & valMask)) << (i * 7)
		if b&hasNextMask != hasNextMask {
			break
		}
	}

	return value, i + 1, nil
}

type TransactionMessageHeader struct {
	NumRequiredSignatures      uint8
	NumReadonlySignedAccounts  uint8
	NumReadonlyUnsingedAcconts uint8
}

type CompiledInstruction struct {
	ProgramIdIndex uint8
	Accounts       []uint8
	Data           []byte
}

func parseCompiledIntruction(data []byte) (*CompiledInstruction, int, error) {
	if len(data) == 0 {
		return nil, 0, errors.New("unable to parse ix")
	}

	accountsCount, accountsByteLen, err := parseCompactUint16(data[1:])
	if err != nil {
		return nil, 0, fmt.Errorf("unable to parse ix accounts: %w", err)
	}

	cix := &CompiledInstruction{
		ProgramIdIndex: data[0],
		Accounts:       make([]uint8, accountsCount),
	}

	offset := accountsByteLen + 1
	if len(data) < offset+int(accountsCount) {
		return nil, 0, errors.New("unable to parse ix accounts")
	}
	for i := range accountsCount {
		cix.Accounts[i] = data[offset]
		offset += 1
	}

	dataLen, dataByteLen, err := parseCompactUint16(data[offset:])
	if err != nil {
		return nil, 0, fmt.Errorf("unable to parse ix data len: %w", err)
	}
	offset += dataByteLen
	if len(data) < offset {
		return nil, 0, errors.New("unable to parse ix data")
	}
	cix.Data = data[offset : offset+int(dataLen)]

	return cix, offset + int(dataLen), nil
}

type CompiledTransactionMessage struct {
	Header          *TransactionMessageHeader
	RecentBlockhash string
	AccountKeys     []string
	Instructions    []*CompiledInstruction
}

func parseTransactionMessage(data []byte) (*CompiledTransactionMessage, error) {
	if len(data) == 0 {
		return nil, errors.New("unable to parse tx message")
	}

	if data[0]&0b10000000 == 0b10000000 {
		data = data[1:]
	}

	if len(data) < 3 {
		return nil, errors.New("unable to parse message header")
	}

	header := &TransactionMessageHeader{
		NumRequiredSignatures:      data[0],
		NumReadonlySignedAccounts:  data[1],
		NumReadonlyUnsingedAcconts: data[2],
	}

	data = data[3:]
	if len(data) < 32 {
		return nil, errors.New("unable to parse message blockhash")
	}

	accountKeysCount, accountsCountLen, err := parseCompactUint16(data)
	if err != nil {
		return nil, fmt.Errorf("unable to parse message accounts counts: %w", err)
	}

	data = data[accountsCountLen:]
	accounts := make([]string, accountKeysCount)
	for i := range accountKeysCount {
		if len(data) < 32 {
			return nil, errors.New("unable to parse message accounts")
		}
		accounts[i] = base58.Encode(data[:32])
		data = data[32:]
	}

	blockhash := base58.Encode(data[:32])
	data = data[32:]

	ixsCount, ixsCountByteLen, err := parseCompactUint16(data)
	if err != nil {
		return nil, errors.New("unable to parse message ixs count")
	}

	ixs := make([]*CompiledInstruction, ixsCount)
	data = data[ixsCountByteLen:]
	for i := range ixsCount {
		ix, byteLen, err := parseCompiledIntruction(data)
		if err != nil {
			return nil, fmt.Errorf("unable to parse message: %w", err)
		}
		ixs[i] = ix
		data = data[byteLen:]
	}

	msg := &CompiledTransactionMessage{
		Header:          header,
		RecentBlockhash: blockhash,
		AccountKeys:     accounts,
		Instructions:    ixs,
	}

	return msg, nil
}

type CompiledTransaction struct {
	Signatures []string
	Message    *CompiledTransactionMessage
}

var ERROR_PARSE_TRANSACTION = errors.New("unable to parse transaction")

func ParseTransaction(data string) (*CompiledTransaction, error) {
	bytes, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: unable to decode transaction data: %w",
			ERROR_PARSE_TRANSACTION, err,
		)
	}

	signaturesCount, offset, err := parseCompactUint16(bytes[:3])
	if err != nil {
		return nil, fmt.Errorf(
			"%w: unable to parse signatures: %w",
			ERROR_PARSE_TRANSACTION, err,
		)
	}

	signatures := make([]string, signaturesCount)
	for i := range signaturesCount {
		sig := base58.Encode(bytes[offset : offset+64])
		signatures[i] = sig
		offset += 64
	}

	msg, err := parseTransactionMessage(bytes[offset:])
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ERROR_PARSE_TRANSACTION, err)
	}

	tx := &CompiledTransaction{
		Signatures: signatures,
		Message:    msg,
	}

	return tx, nil
}

type InnerInstruction struct {
	ProgramAddress string   `json:"program"`
	Accounts       []string `json:"accounts"`
	Data           []byte   `json:"data"`
	Logs           []string `json:"logs"`
	ReturnData     []byte   `json:"returnData"`
}

func (iix *InnerInstruction) Unwrap() *Instruction {
	return &Instruction{
		ProgramAddress: iix.ProgramAddress,
		Accounts:       iix.Accounts,
		Data:           iix.Data,
		Logs:           iix.Logs,
		ReturnData:     iix.ReturnData,
	}
}

type Instruction struct {
	ProgramAddress    string              `json:"program"`
	Accounts          []string            `json:"accounts"`
	Data              []byte              `json:"data"`
	Logs              []string            `json:"logs"`
	ReturnData        []byte              `json:"returnData"`
	InnerInstructions []*InnerInstruction `json:"innerInstructions"`
}

func decompileInstruction(
	txAccounts []string,
	programIndex uint8,
	acccountsIndeces []uint8,
	data []byte,
) (*Instruction, error) {
	if int(programIndex) > len(txAccounts)-1 {
		return nil, errors.New("unable to decompile tx instruction")
	}

	ixAccounts := make([]string, len(acccountsIndeces))
	for j, a := range acccountsIndeces {
		if int(a) > len(txAccounts)-1 {
			return nil, errors.New("unable to decompile ix accounts")
		}
		ixAccounts[j] = txAccounts[a]
	}

	ix := &Instruction{
		ProgramAddress: txAccounts[programIndex],
		Accounts:       ixAccounts,
		Data:           data,
	}
	return ix, nil
}

type NativeBalance struct {
	Pre  uint64 `json:"pre"`
	Post uint64 `json:"post"`
}

type TokenBalanceAmounts struct {
	Pre   uint64 `json:"pre"`
	Post  uint64 `json:"post"`
	Token string `json:"token"`
}

type TokenBalance struct {
	Tokens []*TokenBalanceAmounts `json:"tokens"`
	Owner  string                 `json:"owner"`
}

type Transaction struct {
	Signature      string
	Err            bool
	Fee            uint64
	Accounts       []string
	Blocktime      time.Time
	Slot           uint64
	Ixs            []*Instruction
	NativeBalances map[string]*NativeBalance
	TokenBalances  map[string]*TokenBalance
	TokenDecimals  map[string]uint8
}

func (tx *Transaction) parseBalances(meta *TransactionMeta) error {
	assert.True(tx.Signature != "", "tx is not decompiled")

	tx.NativeBalances = make(map[string]*NativeBalance)
	for ai, balance := range meta.PreBalances {
		account := tx.Accounts[ai]
		postBalance := meta.PostBalances[ai]
		tx.NativeBalances[account] = &NativeBalance{
			Pre:  balance,
			Post: postBalance,
		}
	}

	tx.TokenDecimals = make(map[string]uint8)
	tx.TokenBalances = make(map[string]*TokenBalance)

	for _, tb := range meta.PreTokenBalances {
		tx.TokenDecimals[tb.Mint] = tb.UiTokenAmount.Decimals

		account := tx.Accounts[tb.AccountIndex]
		amount, err := strconv.ParseUint(tb.UiTokenAmount.Amount, 10, 64)
		assert.NoErr(err, "unable to parse token balance")

		etb, ok := tx.TokenBalances[account]
		if !ok {
			etb = &TokenBalance{
				Owner:  tb.Owner,
				Tokens: make([]*TokenBalanceAmounts, 0),
			}
		}

		etb.Tokens = append(etb.Tokens, &TokenBalanceAmounts{
			Pre:   amount,
			Token: tb.Mint,
		})
		tx.TokenBalances[account] = etb
	}

	for _, ptb := range meta.PostTokenBalances {
		tx.TokenDecimals[ptb.Mint] = ptb.UiTokenAmount.Decimals

		account := tx.Accounts[ptb.AccountIndex]
		amount, err := strconv.ParseUint(ptb.UiTokenAmount.Amount, 10, 64)
		assert.NoErr(err, "unable to parse token balance")

		etb, ok := tx.TokenBalances[account]

		switch {
		case !ok:
			etb = &TokenBalance{
				Owner:  ptb.Owner,
				Tokens: make([]*TokenBalanceAmounts, 0),
			}
			fallthrough
		case len(etb.Tokens) == 0:
			etb.Tokens = append(etb.Tokens, &TokenBalanceAmounts{
				Post:  amount,
				Token: ptb.Mint,
			})
		default:
			inserted := false
			for _, t := range etb.Tokens {
				if t.Token == ptb.Mint {
					t.Post = amount
					inserted = true
					break
				}
			}

			if !inserted {
				etb.Tokens = append(etb.Tokens, &TokenBalanceAmounts{
					Post:  amount,
					Token: ptb.Mint,
				})
			}
		}

		tx.TokenBalances[account] = etb
	}

	return nil
}

func parseDataLog(msg string) (data string, ok bool) {
	const (
		PROGRAM_LOG  = "Program log: "
		PROGRAM_DATA = "Program data: "
	)

	if strings.HasPrefix(msg, PROGRAM_LOG) {
		data = strings.Split(msg, PROGRAM_LOG)[1]
		ok = true
	} else if strings.HasPrefix(msg, PROGRAM_DATA) {
		data = strings.Split(msg, PROGRAM_DATA)[1]
		ok = true
	}

	return
}

func parseReturnLog(msg string) (data []byte, ok bool) {
	const PROGRAM_RETURN = "Program return: "

	if strings.HasPrefix(msg, PROGRAM_RETURN) {
		parts := strings.Split(msg, " ")
		var err error
		data, err = base64.StdEncoding.DecodeString(parts[3])
		if err == nil {
			ok = true
		}
	}

	return
}

func (tx *Transaction) parseLogs(logMessages []string) error {
	const (
		INVOKE   = "invoke"
		SUCCESS  = "success"
		TRUNCATE = "Log truncated"
	)

	depth, ixIdx, iixIdx := 0, -1, -1

	for _, msg := range logMessages {
		if msg == TRUNCATE {
			fmt.Printf("Logs truncated for: %s\n", tx.Signature)
			break
		}

		if dataMsg, ok := parseDataLog(msg); ok {
			ix := tx.Ixs[ixIdx]

			if depth > 1 {
				iix := ix.InnerInstructions[iixIdx]
				iix.Logs = append(iix.Logs, dataMsg)
			} else {
				ix.Logs = append(ix.Logs, dataMsg)
			}

			continue
		} else if dataReturn, ok := parseReturnLog(msg); ok {
			ix := tx.Ixs[ixIdx]

			if depth > 1 {
				iix := ix.InnerInstructions[iixIdx]
				iix.ReturnData = dataReturn
			} else {
				ix.ReturnData = dataReturn
			}

			continue
		}

		parts := strings.Split(msg, " ")
		if len(parts) < 3 {
			continue
		}

		switch parts[2] {
		case INVOKE:
			depth += 1
			if depth > 1 {
				iixIdx += 1
			} else {
				ixIdx += 1
			}
		case SUCCESS:
			depth -= 1
			if depth == 0 {
				iixIdx = -1
			}
		}
	}

	return nil
}

var ERROR_DECOMPILE_TRANSACTION = errors.New("unable to decompile tx")

func DecompileTransaction(
	slot uint64,
	blockTime int64,
	meta *TransactionMeta,
	compiled *CompiledTransaction,
) (*Transaction, error) {
	accounts := compiled.Message.AccountKeys
	accounts = append(accounts, meta.LoadedAddresses.Writable...)
	accounts = append(accounts, meta.LoadedAddresses.Readonly...)

	ixs := make([]*Instruction, len(compiled.Message.Instructions))
	for i, cix := range compiled.Message.Instructions {
		ix, err := decompileInstruction(
			accounts,
			cix.ProgramIdIndex,
			cix.Accounts,
			cix.Data,
		)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to decompile ix: %w", ERROR_DECOMPILE_TRANSACTION, err)
		}
		ixs[i] = ix
	}

	for _, innerInstructions := range meta.InnerInstructions {
		ix := ixs[innerInstructions.Index]
		ix.InnerInstructions = make(
			[]*InnerInstruction,
			len(innerInstructions.Instructions),
		)

		for j, ciix := range innerInstructions.Instructions {
			data, err := base58.Decode(ciix.Data)
			if err != nil {
				return nil, fmt.Errorf(
					"%w: unable to decode inner ix data: %w",
					ERROR_DECOMPILE_TRANSACTION, err,
				)
			}
			iix, err := decompileInstruction(
				accounts,
				ciix.ProgramIdIndex,
				ciix.Accounts,
				data,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"%w: unable to decompile inner ix: %w",
					ERROR_DECOMPILE_TRANSACTION, err,
				)
			}
			ix.InnerInstructions[j] = &InnerInstruction{
				ProgramAddress: iix.ProgramAddress,
				Accounts:       iix.Accounts,
				Data:           iix.Data,
			}
		}
	}

	bt := time.Unix(blockTime, 0)
	tx := &Transaction{
		Signature: compiled.Signatures[0],
		Err:       meta.Err != nil,
		Fee:       meta.Fee,
		Accounts:  accounts,
		Blocktime: bt,
		Slot:      slot,
		Ixs:       ixs,
	}
	if err := tx.parseBalances(meta); err != nil {
		return nil, fmt.Errorf(
			"%w: unable to parse tx balances: %w",
			ERROR_DECOMPILE_TRANSACTION, err,
		)
	}

	if !tx.Err {
		if err := tx.parseLogs(meta.LogMessages); err != nil {
			return nil, fmt.Errorf(
				"%w: unable to parse tx logs: %w",
				ERROR_DECOMPILE_TRANSACTION, err,
			)
		}
	}

	return tx, nil
}
