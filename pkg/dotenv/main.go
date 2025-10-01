package dotenv

import (
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"taxee/pkg/assert"
)

func ReadEnv() error {
	_, file, _, ok := runtime.Caller(0)
	assert.True(ok, "unable to get caller")

	dotenvPath := path.Join(file, "../../../.env")
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
