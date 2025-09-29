package db

import (
	"context"
	"fmt"
	"os"
	"taxee/pkg/assert"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	Server   string
	Port     string
	Db       string
	User     string
	Password string
}

func ReadConfig() Config {
	dbServer := os.Getenv("DB_SERVER")
	assert.True(len(dbServer) > 0, "DB_SERVER not set")
	dbPort := os.Getenv("DB_PORT")
	assert.True(len(dbPort) > 0, "DB_PORT not set")
	db := os.Getenv("DB")
	assert.True(len(db) > 0, "DB not set")
	dbUser := os.Getenv("DB_USER")
	assert.True(len(dbUser) > 0, "DB_USER not set")
	dbPassword := os.Getenv("DB_PASSWORD")
	assert.True(len(dbPassword) > 0, "DB_PASSWORD not set")

	return Config{dbServer, dbPort, db, dbUser, dbPassword}
}

func InitPool(ctx context.Context, appEnv string) (*pgxpool.Pool, error) {
	dbConfig := ReadConfig()

	// TODO: ?? check what it should be
	sslMode := "enable"
	if appEnv != "prod" {
		sslMode = "disable"
	}

	pool, err := pgxpool.New(
		ctx,
		fmt.Sprintf(
			"postgresql://%s:%s@%s:%s/%s?sslmode=%s",
			dbConfig.User,
			dbConfig.Password,
			dbConfig.Server,
			dbConfig.Port,
			dbConfig.Db,
			sslMode,
		),
	)

	return pool, err
}
