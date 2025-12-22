package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"taxee/cmd/fetcher"
	"taxee/cmd/fetcher/evm"
	"taxee/cmd/fetcher/solana"
	"taxee/cmd/parser"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"
	"taxee/pkg/db"
	"taxee/pkg/dotenv"
	requesttimer "taxee/pkg/request_timer"

	"golang.org/x/crypto/sha3"

	"github.com/jackc/pgx/v5"
)

func goSliceString(s []byte, size int) string {
	sb := strings.Builder{}
	sb.WriteRune('[')
	if size != -1 {
		sb.WriteString(fmt.Sprintf("%d", size))
	}
	sb.WriteString("]uint8{")

	for i, b := range s {
		sb.WriteString(fmt.Sprintf("%d", b))
		if i != len(s)-1 {
			sb.WriteRune(',')
		}
	}
	sb.WriteRune('}')

	return sb.String()
}

type abiInput struct {
	InternalType string
	Name         string
	Type         string
	Components   []abiInput
}

type abiComponent struct {
	Inputs []abiInput
	Name   string
	Type   string
}

func abiProcessInputs(inputs []abiInput) string {
	s := strings.Builder{}

	for i, input := range inputs {
		if len(input.Components) > 0 {
			s.WriteRune('(')
			s.WriteString(abiProcessInputs(input.Components))
			s.WriteRune(')')
		} else {
			s.WriteString(input.Type)
		}

		if i < len(inputs)-1 {
			s.WriteRune(',')
		}
	}

	return s.String()
}

func abiProcessMethods(
	abi []abiComponent,
) (signatures map[string]string, selectors map[string]uint32) {
	signatures = make(map[string]string)
	selectors = make(map[string]uint32)

	for _, component := range abi {
		if component.Type != "function" {
			continue
		}

		signature := strings.Builder{}
		signature.WriteString(component.Name)
		signature.WriteRune('(')
		signature.WriteString(abiProcessInputs(component.Inputs))
		signature.WriteRune(')')
		signatureStr := signature.String()

		funcName := fmt.Sprintf(
			"%s%s",
			strings.ToUpper(string(component.Name[0])),
			component.Name[1:],
		)

		hasher := sha3.NewLegacyKeccak256()
		hasher.Write([]byte(signatureStr))
		selector := binary.BigEndian.Uint32(hasher.Sum(nil)[:4])

		signatures[funcName] = signatureStr
		selectors[funcName] = selector
	}

	return
}

func main() {
	appEnv := os.Getenv("APP_ENV")
	if appEnv != "prod" {
		assert.NoErr(dotenv.ReadEnv(), "")
	}

	cliArgs := os.Args
	assert.True(len(cliArgs) > 1, "Need to specify command")

	pool, err := db.InitPool(context.Background(), appEnv)
	assert.NoErr(err, "")

	switch cliArgs[1] {
	case "abi-extract-methods":
		assert.True(len(cliArgs) > 2, "Missing contract name")
		contractName := cliArgs[2]

		const capacity = 10 * 1024 * 1024
		reader := bufio.NewReaderSize(os.Stdin, capacity)
		abiBytes, err := reader.ReadBytes('\n')
		assert.True(err == nil || errors.Is(err, io.EOF), "unable to read ABI")

		var data []abiComponent
		err = json.Unmarshal(abiBytes, &data)
		assert.NoErr(err, "")

		signatures, selectors := abiProcessMethods(data)
		var contractId uint32

		for funcName, signature := range signatures {
			selector := selectors[funcName]
			contractId ^= selector

			varDef := strings.Builder{}
			varDef.WriteString("evm")
			varDef.WriteString(contractName)
			varDef.WriteString(funcName)
			varDef.WriteString(" uint32 = 0x")
			varDef.WriteString(fmt.Sprintf("%x", selector))
			varDef.WriteString(" // ")
			varDef.WriteString(signature)
			fmt.Println(varDef.String())
		}

		fmt.Printf("evm%sContractId uint32 = 0x%x", contractName, contractId)
	case "keccak":
		assert.True(len(cliArgs) > 2, "Missing method")

		method := cliArgs[2]
		fmt.Println(method)
		hasher := sha3.NewLegacyKeccak256()
		hasher.Write([]byte(method))
		hash := hasher.Sum(nil)[:4]

		fmt.Println(goSliceString(hash, 4))

		h := hex.EncodeToString(hash)
		fmt.Printf("0x%s\n", h)

		fmt.Printf("LE: %d\n", binary.LittleEndian.Uint32(hash))
		fmt.Printf("BE: %d\n", binary.BigEndian.Uint32(hash))

	case "parse-hex":
		assert.True(len(cliArgs) > 2, "Missing hex discriminator")

		decoded, err := hex.DecodeString(cliArgs[2])
		assert.NoErr(err, "invalid hex")

		fmt.Println(goSliceString(decoded, -1))
	case "migrate":
		assert.True(len(cliArgs) > 2, "Need to specify migration path")
		migrationPath := cliArgs[2]

		fmt.Printf("Executing commands in file: \"%s\"\n\n", migrationPath)
		dbConfig := db.ReadConfig()

		psqlCmd := fmt.Sprintf(
			"psql -U %s -h %s -p %s -d %s -f %s",
			dbConfig.User, dbConfig.Server, dbConfig.Port, dbConfig.Db, migrationPath,
		)
		cmd := exec.Command("bash", "-c", psqlCmd)
		cmd.Env = append(
			cmd.Env,
			fmt.Sprintf("PGPASSWORD=%s", dbConfig.Password),
		)

		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Println("Failed to migrate:")
			fmt.Println(string(output))
			os.Exit(1)
		} else {
			fmt.Println(string(output))
		}
	case "seed-coingecko":
		coingecko.Init()
		parser.FetchCoingeckoTokens(context.Background(), pool)
	case "fetch":
		// fetch txs
		assert.True(appEnv != "prod", "this command must not be run in production env")
		assert.True(len(cliArgs) > 2, "Need to provide wallet address")
		assert.True(len(cliArgs) > 3, "Need to provide network")

		userAccountId := int32(1)
		_, err := db.GetUserAccount(context.Background(), pool, userAccountId)
		if errors.Is(err, pgx.ErrNoRows) {
			_, err = db.InsertUserAccount(context.Background(), pool, "testing123")
			assert.NoErr(err, "")
		}

		walletAddress, network := cliArgs[2], cliArgs[3]
		validNetwork, ok := db.NewNetwork(network)
		assert.True(ok, "network is not valid: %s", network)

		alchemyReqTimer := requesttimer.NewDefault(100)

		solanaRpc := solana.NewRpc(alchemyReqTimer)
		etherscanClient := evm.NewClient(alchemyReqTimer)

		fresh := false
		if len(cliArgs) > 4 && cliArgs[4] == "fresh" {
			fresh = true
		}

		walletId, walletData, err := db.DevSetWallet(
			context.Background(), pool,
			userAccountId, walletAddress, validNetwork,
		)
		assert.NoErr(err, "unable to set wallet")

		err = fetcher.Fetch(
			context.Background(),
			pool, solanaRpc, etherscanClient,
			userAccountId,
			validNetwork, walletAddress, walletId, walletData,
			fresh,
		)
		assert.NoErr(err, "unable to fetch transactions for wallet")
	case "parse":
		assert.True(appEnv != "prod", "this command must not be run in production env")

		coingecko.Init()

		alchemyReqTimer := requesttimer.NewDefault(100)
		etherscanClient := evm.NewClient(alchemyReqTimer)

		parser.Parse(context.Background(), pool, etherscanClient, 1)
	}
}
