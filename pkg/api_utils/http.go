package apiutils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

func HttpSend(
	ctx context.Context,
	request *http.Request,
	result any,
) (statusCode int, statusOk bool, err error) {
	var res *http.Response
	res, err = http.DefaultClient.Do(request.WithContext(ctx))
	if err != nil {
		err = fmt.Errorf("unable to send request: %w", err)
		return
	}
	defer res.Body.Close()

	statusCode = res.StatusCode
	statusOk = res.StatusCode >= 200 && res.StatusCode < 300

	var buf bytes.Buffer
	chunk := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}
		var n int
		n, err = res.Body.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			err = fmt.Errorf("stream error after reading %d bytes: %w", buf.Len(), err)
			return
		}
	}

	resBody := buf.Bytes()

	if err = json.Unmarshal(resBody, result); err != nil {
		err = fmt.Errorf("unable to unmarshal: %w\nBody: %s", err, string(resBody))
	}

	return
}
