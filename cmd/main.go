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
	"github.com/jackc/pgx/v5/pgconn"
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

		type abiInput struct {
			InternalType string
			Name         string
			Type         string
		}

		type abiComponent struct {
			Inputs []abiInput
			Name   string
			Type   string
		}

		var data []abiComponent
		err = json.Unmarshal(abiBytes, &data)
		assert.NoErr(err, "")

		signatures := make(map[string]string)
		contractId := uint32(0)

		for _, comp := range data {
			if comp.Type != "function" {
				continue
			}

			signature := strings.Builder{}
			signature.WriteString(comp.Name)
			signature.WriteRune('(')

			for i, inp := range comp.Inputs {
				signature.WriteString(inp.Type)
				signature.WriteRune(' ')
				signature.WriteString(inp.Name)
				if i < len(comp.Inputs)-1 {
					signature.WriteRune(',')
				}
			}

			signature.WriteRune(')')

			sigString := signature.String()

			hasher := sha3.NewLegacyKeccak256()
			hasher.Write([]byte(sigString))
			hash := hasher.Sum(nil)[:4]

			name := fmt.Sprintf(
				"evm%s%s%s",
				contractName, strings.ToUpper(string(comp.Name[0])), comp.Name[1:],
			)
			signatures[name] = fmt.Sprintf(
				"0x%s",
				hex.EncodeToString(hash),
			)

			contractId ^= binary.BigEndian.Uint32(hash)
		}

		for signature, hash := range signatures {
			fmt.Printf("%s uint32 = %s\n", signature, hash)
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
	case "seed":
		coingecko.Init()

		coins, err := coingecko.GetCoins()
		assert.NoErr(err, "unable to get coingecko coins")

		batch := pgx.Batch{}
		for _, coin := range coins {
			q := db.EnqueueInsertCoingeckoTokenData(
				&batch,
				coin.Id,
				coin.Symbol,
				coin.Name,
			)
			q.Exec(func(ct pgconn.CommandTag) error { return nil })

			for platform, mint := range coin.Platforms {
				switch platform {
				case "solana":
					q := db.EnqueueInsertCoingeckoToken(
						&batch,
						coin.Id,
						db.NetworkSolana,
						mint,
					)
					q.Exec(func(_ pgconn.CommandTag) error { return nil })
				}
			}
		}

		br := pool.SendBatch(context.Background(), &batch)
		assert.NoErr(br.Close(), "")
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

		alchemyReqTimer := requesttimer.NewDefault(100)

		solanaRpc := solana.NewRpc(alchemyReqTimer)
		etherscanClient := evm.NewClient(alchemyReqTimer)

		fresh := false
		if len(cliArgs) > 4 && cliArgs[4] == "fresh" {
			fresh = true
		}

		fetcher.Fetch(
			context.Background(),
			pool,
			userAccountId,
			walletAddress,
			network,
			solanaRpc,
			etherscanClient,
			fresh,
		)
	case "parse":
		assert.True(appEnv != "prod", "this command must not be run in production env")

		coingecko.Init()

		alchemyReqTimer := requesttimer.NewDefault(100)
		etherscanClient := evm.NewClient(alchemyReqTimer)

		parser.Parse(context.Background(), pool, etherscanClient, 1, true)
	case "parse-server":
		// long running server
	}
}
