package solana

import (
	"encoding/base64"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCompactUint16(t *testing.T) {
	b := []byte{0b10000001, 0b01111111}
	val, c, err := parseCompactUint16(b)
	require.Nil(t, err)
	require.Equal(t, 2, c)

	// 0b0011111110000001
	expected := 0b00000001 + (0b01111111 << 7)
	require.Equal(t, uint16(expected), val)

	b = []byte{0b11111111, 0b11111111, 0b00000011}
	val, c, err = parseCompactUint16(b)
	require.Nil(t, err)
	require.Equal(t, 3, c)
	require.Equal(t, uint16(math.MaxUint16), val)

	b = []byte{0b01111111}
	val, c, err = parseCompactUint16(b)
	require.Nil(t, err)
	require.Equal(t, 1, c)
	require.Equal(t, uint16(127), val)
}

func TestParseBalances(t *testing.T) {
	meta := &TransactionMeta{
		PreBalances:  []uint64{0, 150, 100},
		PostBalances: []uint64{100, 0, 160},
		PreTokenBalances: []*TransactionTokenBalance{
			{AccountIndex: 0, Mint: "m1", Owner: "o1", UiTokenAmount: &TransactionUiTokenAmount{Amount: "100", Decimals: 2}},
			{AccountIndex: 0, Mint: "m2", Owner: "o1", UiTokenAmount: &TransactionUiTokenAmount{Amount: "150", Decimals: 2}},
			{AccountIndex: 1, Mint: "m2", Owner: "o1", UiTokenAmount: &TransactionUiTokenAmount{Amount: "150", Decimals: 3}},
		},
		PostTokenBalances: []*TransactionTokenBalance{
			{AccountIndex: 1, Mint: "m2", Owner: "o1", UiTokenAmount: &TransactionUiTokenAmount{Amount: "200", Decimals: 3}},
			{AccountIndex: 2, Mint: "m3", Owner: "o1", UiTokenAmount: &TransactionUiTokenAmount{Amount: "500", Decimals: 4}},
		},
	}
	tx := &Transaction{
		Signature: "123",
		Accounts:  []string{"a1", "a2", "a3"},
	}
	err := tx.parseBalances(meta)
	assert.Nil(t, err)

	expectedDecimals := map[string]uint8{
		"m1": 2,
		"m2": 3,
		"m3": 4,
	}
	require.Equal(t, expectedDecimals, tx.TokenDecimals)

	expectedNative := map[string]*NativeBalance{
		"a1": {Pre: 0, Post: 100},
		"a2": {Pre: 150, Post: 0},
		"a3": {Pre: 100, Post: 160},
	}
	require.Equal(t, expectedNative, tx.NativeBalances)

	expectedTokens := map[string]*TokenBalance{
		"a1": {
			Owner: "o1",
			Tokens: []*TokenBalanceAmounts{
				{
					Pre:   100,
					Post:  0,
					Token: "m1",
				},
				{
					Pre:   150,
					Post:  0,
					Token: "m2",
				},
			},
		},
		"a2": {
			Owner:  "o1",
			Tokens: []*TokenBalanceAmounts{{Pre: 150, Post: 200, Token: "m2"}},
		},
		"a3": {
			Owner:  "o1",
			Tokens: []*TokenBalanceAmounts{{Pre: 0, Post: 500, Token: "m3"}},
		},
		// "a1": {Pre: 100, Post: 0, Token: "m1", Owner: "o1"},
		// "a2": {Pre: 150, Post: 200, Token: "m2", Owner: "o1"},
		// "a3": {Pre: 0, Post: 500, Token: "m3", Owner: "o1"},
	}
	require.Equal(t, expectedTokens, tx.TokenBalances)
}

func TestParseLogs(t *testing.T) {
	ret := []byte{1, 2, 3}
	logMessages := []string{
		// first
		"Program ComputeBudget111111111111111111111111111111 invoke [1]",
		"Program ComputeBudget111111111111111111111111111111 success",

		// second
		"Program SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE invoke [1]",
		"Program log: Instruction: IdleToLoadingBay",

		// second -> inner
		"Program SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE invoke [2]",
		"Program data: 123",
		fmt.Sprintf("Program return: <program> %s", base64.StdEncoding.EncodeToString(ret)),
		"Program SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE success",

		"Program log: Current state: Idle(Idle { sector: [23, -12] })",
		"Program SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE consumed 11334 of 602850 compute units",
		"Program SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE success",

		// third
		"Program SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE invoke [1]",
		"Program log: Instruction: IdleToLoadingBay",

		// third -> inner
		"Program SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE invoke [2]",
		"Program data: 123",
		fmt.Sprintf("Program return: <program> %s", base64.StdEncoding.EncodeToString(ret)),
		"Program SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE success",

		"Program log: Current state: Idle(Idle { sector: [23, -12] })",
		"Program SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE consumed 11334 of 602850 compute units",
		"Program SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE success",
	}

	tx := &Transaction{
		Signature: "123",
		Ixs: []*Instruction{
			{},
			{
				InnerInstructions: []*InnerInstruction{{}},
			},
			{
				InnerInstructions: []*InnerInstruction{{}},
			},
		},
	}
	err := tx.parseLogs(logMessages)
	require.Nil(t, err)

	expected := &Transaction{
		Signature: "123",
		Ixs: []*Instruction{
			{},
			{
				Logs: []string{
					"Instruction: IdleToLoadingBay",
					"Current state: Idle(Idle { sector: [23, -12] })",
				},
				InnerInstructions: []*InnerInstruction{{
					Logs:       []string{"123"},
					ReturnData: ret,
				}},
			},
			{
				Logs: []string{
					"Instruction: IdleToLoadingBay",
					"Current state: Idle(Idle { sector: [23, -12] })",
				},
				InnerInstructions: []*InnerInstruction{{
					Logs:       []string{"123"},
					ReturnData: ret,
				}},
			},
		},
	}
	require.Equal(t, expected, tx)
}
