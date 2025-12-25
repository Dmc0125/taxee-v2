package solana

import (
	"encoding/json"
	"errors"
	"fmt"
	"taxee/pkg/jsonrpc"
)

const AlchemyApiUrl = "https://solana-mainnet.g.alchemy.com/v2/"

var AlchemyMethodsCosts = map[string]int{
	GetSignaturesForAddress: 40,
	GetTransaction:          20,
	GetBlock:                40,
}

const (
	GetSignaturesForAddress = "getSignaturesForAddress"
	GetTransaction          = "getTransaction"
	GetBlock                = "getBlock"
)

type Commitment string

const (
	CommitmentProcessed Commitment = "processed"
	CommitmentConfirmed Commitment = "confirmed"
	CommitmentFinalized Commitment = "finalized"
)

func (c *Commitment) UnmarshalJSON(src []byte) error {
	if len(src) < 2 {
		return fmt.Errorf("invalid commitment: %s", string(src))
	}
	switch Commitment(src[1 : len(src)-1]) {
	case CommitmentProcessed:
		*c = CommitmentProcessed
	case CommitmentConfirmed:
		*c = CommitmentConfirmed
	case CommitmentFinalized:
		*c = CommitmentFinalized
	default:
		return fmt.Errorf("invalid commitment: %s", string(src))
	}
	return nil
}

type GetSignaturesForAddressConfig struct {
	Commitment Commitment `json:"commitment"`
	Limit      uint16     `json:"limit,omitempty"`
	Before     string     `json:"before,omitempty"`
	Until      string     `json:"until,omitempty"`
}

type GetSignaturesForAddressParams struct {
	Address string
	Config  *GetSignaturesForAddressConfig
}

func (p *GetSignaturesForAddressParams) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{p.Address, p.Config})
}

type SignatureResult struct {
	Signature          string     `json:"signature"`
	Slot               uint64     `json:"slot"`
	Err                any        `json:"err"`
	BlockTime          int64      `json:"blockTime"`
	ConfirmationStatus Commitment `json:"confirmationStatus"`
}

func (r *SignatureResult) Validate() error {
	if r.Signature == "" {
		return errors.New("missing signature")
	}
	if r.Slot == 0 {
		return errors.New("missing slot")
	}
	if r.BlockTime == 0 {
		return errors.New("missing blockTime")
	}
	if r.ConfirmationStatus == "" {
		return errors.New("missing confirmationStatus")
	}
	return nil
}

type GetSignaturesForAddressResult []SignatureResult

var _ jsonrpc.Validator = (*GetSignaturesForAddressResult)(nil)

func (r *GetSignaturesForAddressResult) Validate() error {
	for _, s := range *r {
		if err := s.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type Encoding string

const (
	EncodingBase64 Encoding = "base64"
)

func (e *Encoding) UnmarshalJSON(src []byte) error {
	if len(src) < 2 {
		return fmt.Errorf("invalid encoding: %s", string(src))
	}

	switch Encoding(src[1 : len(src)-1]) {
	case EncodingBase64:
		*e = EncodingBase64
	default:
		return fmt.Errorf("invalid encoding: %s", string(src))
	}

	return nil
}

type GetTransactionConfig struct {
	Commitment                     Commitment `json:"commitment"`
	MaxSupportedTransactionVersion uint       `json:"maxSupportedTransactionVersion"`
	Encoding                       Encoding   `json:"encoding"`
}

type GetTransactionParams struct {
	Signature string                `json:"signature"`
	Config    *GetTransactionConfig `json:"config"`
}

var _ json.Marshaler = (*GetTransactionParams)(nil)

func (p *GetTransactionParams) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{p.Signature, p.Config})
}

type TransactionLoadedAddress struct {
	Writable []string `json:"writable"`
	Readonly []string `json:"readonly"`
}

type TransactionUiTokenAmount struct {
	Amount   string `json:"amount"`
	Decimals uint8  `json:"decimals"`
}

type TransactionTokenBalance struct {
	AccountIndex  uint8                     `json:"accountIndex"`
	Mint          string                    `json:"mint"`
	Owner         string                    `json:"owner"`
	ProgramId     string                    `json:"programId"`
	UiTokenAmount *TransactionUiTokenAmount `json:"uiTokenAmount"`
}

type CompiledInnerInstruction struct {
	ProgramIdIndex uint8   `json:"programIdIndex"`
	Accounts       []uint8 `json:"accounts"`
	Data           string  `json:"data"`
}

type TransactionInnerInstructions struct {
	Index        int                         `json:"index"`
	Instructions []*CompiledInnerInstruction `json:"instructions"`
}

type TransactionMeta struct {
	Err               any                             `json:"err"`
	Fee               uint64                          `json:"fee"`
	PostBalances      []uint64                        `json:"postBalances"`
	PreBalances       []uint64                        `json:"preBalances"`
	LogMessages       []string                        `json:"logMessages"`
	Version           any                             `json:"version"`
	LoadedAddresses   *TransactionLoadedAddress       `json:"loadedAddresses"`
	PreTokenBalances  []*TransactionTokenBalance      `json:"preTokenBalances"`
	PostTokenBalances []*TransactionTokenBalance      `json:"postTokenBalances"`
	InnerInstructions []*TransactionInnerInstructions `json:"innerInstructions"`
}

type GetTransactionResult struct {
	BlockTime   int64            `json:"blockTime"`
	Meta        *TransactionMeta `json:"meta"`
	Slot        uint64           `json:"slot"`
	Transaction [2]string        `json:"transaction"`
	Version     any              `json:"version"`
}

var _ jsonrpc.Validator = (*GetTransactionResult)(nil)

func (r *GetTransactionResult) Validate() error {
	if r.BlockTime == 0 {
		return errors.New("missing blockTime")
	}
	if r.Meta == nil {
		// TODO: Actual validation
		return errors.New("missing transaction meta")
	}
	if r.Slot == 0 {
		return errors.New("missing slot")
	}

	if r.Transaction[0] == "" {
		return errors.New("missing transaction")
	}
	if r.Transaction[1] == "" {
		return errors.New("missing transaction encoding")
	}

	switch v := r.Version.(type) {
	case string:
		if v != "legacy" {
			return fmt.Errorf("invalid version: %s", v)
		}
	case float64:
		if v != 0 {
			return fmt.Errorf("invalid version: %f", v)
		}
	default:
		return fmt.Errorf("invalid version: %T (%#v)", v, v)
	}

	return nil
}

const (
	TransactionDetailsSignatures = "signatures"
)

type GetBlockConfig struct {
	Commitment                     Commitment `json:"commitment"`
	Encoding                       Encoding   `json:"encoding"`
	TransactionDetails             string     `json:"transactionDetails"`
	MaxSupportedTransactionVersion int        `json:"maxSupportedTransactionVersion"`
	Rewards                        bool       `json:"rewards"`
}

type GetBlockParams struct {
	Slot   uint64
	Config *GetBlockConfig
}

func (p *GetBlockParams) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{p.Slot, p.Config})
}

type GetBlockResult struct {
	BlockHeight       uint64   `json:"blockHeight"`
	BlockTime         int64    `json:"blockTime"`
	Blockhash         string   `json:"blockhash"`
	ParentSlot        uint64   `json:"parentSlot"`
	PreviousBlockhash string   `json:"previousBlockhash"`
	Signatures        []string `json:"signatures"`
}

var _ jsonrpc.Validator = (*GetBlockResult)(nil)

func (r *GetBlockResult) Validate() error {
	if r.BlockHeight == 0 {
		return errors.New("missing blockHeight")
	}
	if r.BlockTime == 0 {
		return errors.New("missing blockTime")
	}
	if r.Blockhash == "" {
		return errors.New("missing blockhash")
	}
	if r.ParentSlot == 0 {
		return errors.New("missing parentSlot")
	}
	if r.PreviousBlockhash == "" {
		return errors.New("missing previousBlockhash")
	}
	return nil
}
