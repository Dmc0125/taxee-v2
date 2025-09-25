package assert

import (
	"fmt"
	"os"
	"runtime/debug"
)

var usePanic = false

func SetUsePanic(n bool) {
	usePanic = n
}

func True(cond bool, msg string, args ...any) {
	if !cond {
		fmt.Fprintf(os.Stderr, msg, args...)
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "Stacktrace: %s\n\n", string(debug.Stack()))

		if usePanic {
			panic("Assert True failed")
		} else {
			os.Exit(1)
		}
	}
}

func NoErr(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n%s\nErr: %s\n\n", msg, err)
		fmt.Fprintf(os.Stderr, "Stacktrace: %s\n\n", string(debug.Stack()))

		if usePanic {
			panic("Assert NoErr failed")
		} else {
			os.Exit(1)
		}
	}
}
