package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"taxee/cmd/fetcher"
	"taxee/cmd/fetcher/solana"
	"taxee/cmd/parser"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/dotenv"

	"github.com/jackc/pgx/v5"
)

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
	case "fetch":
		// fetch txs
		assert.True(len(cliArgs) > 2, "Need to provide wallet address")
		assert.True(len(cliArgs) > 3, "Need to provide network")

		userAccountId := int32(1)
		_, err := db.GetUserAccount(context.Background(), pool, userAccountId)
		if errors.Is(err, pgx.ErrNoRows) {
			_, err = db.InsertUserAccount(context.Background(), pool, "testing123")
			assert.NoErr(err, "")
		}

		walletAddress, network := cliArgs[2], cliArgs[3]

		solanaRpcUrl := os.Getenv("SOLANA_RPC_URL")
		rpc := solana.NewRpc(solanaRpcUrl)

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
			rpc,
			fresh,
		)
	case "parse":
		// parse
		parser.Parse(context.Background(), pool, 1)
	case "parse-server":
		// long running server
	}
}
