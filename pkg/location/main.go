package location

import (
	"errors"
	"fmt"
	"runtime"
)

// returns the current code location in this format
// <file>:<function>:<line>
func Location(skip int) (string, error) {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "", errors.New("unable to get caller")
	}

	f := runtime.FuncForPC(pc).Name()

	return fmt.Sprintf("%s:%s:%d", file, f, line), nil
}
