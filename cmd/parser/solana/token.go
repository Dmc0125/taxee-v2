package solana

import (
	"taxee/pkg/db"

	"github.com/mr-tron/base58"
)

const TOKEN_PROGRAM_ADDRESS = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

type tokenIx uint8

const (
	// Accounting
	tokenIxInitializeAccount tokenIx = iota
	tokenIxInitializeAccount2
	tokenIxInitializeAccount3
	tokenIxClose

	// Economic
	tokenIxTransfer tokenIx = 50 + iota
)

func tokenIxFromByte(disc byte) (ix tokenIx, ok bool) {
	ok = true

	switch tokenIx(disc) {
	// Accounting
	case 1:
		ix = tokenIxInitializeAccount
	case 16:
		ix = tokenIxInitializeAccount2
	case 18:
		ix = tokenIxInitializeAccount3
	case 9:
		ix = tokenIxClose

	// Economic
	case 3:
		ix = tokenIxTransfer
	default:
		ok = false
	}

	return
}

func tokenIxRelatedAccounts(
	relatedAccounts relatedAccounts,
	walletAddress string,
	ix *db.SolanaInstruction,
) {
	ixType, ok := tokenIxFromByte(ix.Data[0])
	if !ok || ixType > 50 {
		return
	}

	var owner, tokenAccount string

	switch ixType {
	case tokenIxInitializeAccount:
		tokenAccount, owner = ix.Accounts[0], ix.Accounts[2]
	case tokenIxInitializeAccount2, tokenIxInitializeAccount3:
		tokenAccount = ix.Accounts[0]
		owner = base58.Encode(ix.Data[1:33])
	default:
		return
	}

	if walletAddress == owner {
		relatedAccounts.Append(tokenAccount)
		return
	}
}

type TokenAccountData struct {
	Mint  string
	Owner string
}

func preprocessTokenIx(ctx *Context, ix *db.SolanaInstruction) {
	ixType, ok := tokenIxFromByte(ix.Data[0])
	if !ok || ixType > 50 {
		return
	}

	var tokenAccount, mint, owner string

	switch ixType {
	case tokenIxInitializeAccount:
		tokenAccount, mint, owner = ix.Accounts[0], ix.Accounts[1], ix.Accounts[2]
	case tokenIxInitializeAccount3, tokenIxInitializeAccount2:
		tokenAccount, mint = ix.Accounts[0], ix.Accounts[1]
		owner = base58.Encode(ix.Data[1:33])
	case tokenIxClose:
		tokenAccount, owner = ix.Accounts[0], ix.Accounts[2]
		destination := ix.Accounts[1]

		if !ctx.WalletOwned(owner) {
			return
		}

		ctx.accountClose(tokenAccount, destination)
		return
	default:
		return
	}

	if !ctx.WalletOwned(owner) {
		return
	}

	data := TokenAccountData{
		Mint:  mint,
		Owner: owner,
	}
	ctx.accountInitOwned(tokenAccount, &data)
}
