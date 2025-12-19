package db

import (
	"errors"
	"fmt"
)

type JobType string

const (
	JobFetchWallet       JobType = "fetch_wallet"
	JobParseTransactions JobType = "parse_transactions"
	JobParseEvents       JobType = "parse_events"
)

func (dst *JobType) ParseString(src string) error {
	if len(src) == 0 {
		return errors.New("empty")
	}

	switch JobType(src) {
	case JobParseTransactions, JobParseEvents:
	default:
		return fmt.Errorf("%s is invalid", src)
	}

	*dst = JobType(src)
	return nil
}
