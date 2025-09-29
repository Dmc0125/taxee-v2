package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"taxee/cmd/fetcher"
	"taxee/cmd/fetcher/solana"
	"taxee/cmd/parser"
	"taxee/pkg/assert"
	"taxee/pkg/db"
)

func rootPath() string {
	_, file, _, ok := runtime.Caller(0)
	assert.True(ok, "unable to get caller")
	return path.Join(file, "../..")
}

func readEnv() error {
	dotenvPath := path.Join(rootPath(), "/.env")
	data, err := os.ReadFile(dotenvPath)
	if err != nil {
		return fmt.Errorf("unable to read .env file: %w", err)
	}

	isValidIdent := func(b byte) bool {
		// Nums: 48 <= b <= 57
		// Alpha upper: 65 <= b <= 90
		// Alpha lower: 97 <= b <= 122
		// _ => 95
		isNum := 48 <= b && b <= 57
		isAlpha := (65 <= b && b <= 90) || (97 <= b && b <= 122)
		return isNum || isAlpha || b == 95
	}

	const (
		ident = iota
		value
	)
	next := ident

	key, val := make([]byte, 0), make([]byte, 0)

	write := func() {
		if len(val) > 0 {
			os.Setenv(string(key), string(val))
			key, val = key[:0], val[:0]
		}
	}

chars:
	for _, b := range data {
		switch next {
		case ident:
			if isValidIdent(b) {
				key = append(key, b)
				continue chars
			} else if b == '=' {
				if len(key) == 0 {
					return errors.New("unable to read .env file: expected identifier")
				}
				next = value
			}
		case value:
			if b == '\n' {
				next = ident
				write()
				continue chars
			}
			val = append(val, b)
		}
	}

	if len(key) > 0 && len(val) > 0 {
		write()
	}

	return nil
}

func main() {
	appEnv := os.Getenv("APP_ENV")
	if appEnv != "prod" {
		assert.NoErr(readEnv(), "")
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
			walletAddress,
			network,
			rpc,
			fresh,
		)
	case "parse":
		// parse
		parser.Parse(context.Background(), pool)
	case "parse-server":
		// long running server
	}
}
