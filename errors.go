package gosmsg

import (
	"errors"
	"fmt"
)

var (
	EOS              = errors.New("end of SMSG")
	ErrUnexpectedEnd = errors.New("unexpected end of SMSG")
)

// MissingSchema represents an error when decoding a record type not matching the provided schema
type MissingSchema struct {
	Tag uint16
}

func (e *MissingSchema) Error() string {
	return fmt.Sprintf("tag 0x%04X does not match any schemas", e.Tag)
}
