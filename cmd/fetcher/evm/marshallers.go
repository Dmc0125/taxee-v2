package evm

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

type StringUint64 uint64

func (dst *StringUint64) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	res, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse string as uint64: %s %w", s, err)
	}
	*dst = StringUint64(res)
	return nil
}

type StringUint32 uint32

func (dst *StringUint32) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	res, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return fmt.Errorf("unable to parse string as uint32: %s %w", s, err)
	}
	*dst = StringUint32(res)
	return nil
}

type StringTime time.Time

func (dst *StringTime) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	res, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse string as int64: %s %w", s, err)
	}
	*dst = StringTime(time.Unix(res, 0))
	return nil
}

type StringErr bool

func (dst *StringErr) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	*dst = s != "0"
	return nil
}

type HexBytes []byte

func (dst *HexBytes) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	if s == "0x" || len(s) == 0 {
		return nil
	}

	s = s[2:]
	if len(s)%2 == 1 {
		s = string(append(
			[]byte{'0'},
			s...,
		))
	}

	bytes, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("invalid bytes: %s %w", s, err)
	}
	*dst = bytes
	return nil
}

type HexUint64 uint64

func (dst *HexUint64) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	if s == "0x" {
		return nil
	}

	res, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		return fmt.Errorf("unable to convert string to uint64: %s %w", s, err)
	}
	*dst = HexUint64(res)
	return nil
}

type HexUint32 uint32

func (dst *HexUint32) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	if s == "0x" {
		return nil
	}

	res, err := strconv.ParseInt(s, 0, 32)
	if err != nil {
		return fmt.Errorf("unable to convert string to uint32: %s %w", s, err)
	}
	*dst = HexUint32(res)
	return nil
}

type HexTime time.Time

func (dst *HexTime) UnmarshalJSON(src []byte) error {
	s := string(src[1 : len(src)-1])
	if s == "0x" {
		return nil
	}

	res, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		return fmt.Errorf("unable to convert string to int64: %s %w", s, err)
	}
	*dst = HexTime(time.Unix(res, 0))
	return nil
}
