package gosmsg

import (
	"errors"
	"fmt"
)

var (
	// Indicates normal end of an SMSG
	EOS = errors.New("end of SMSG")
	// Indicates underlying reader returned 0 bytes
	ErrUnexpectedEnd = errors.New("unexpected end of SMSG")
)

// MissingSchemaError represents an error when decoding a record type not matching the provided schema
type MissingSchemaError struct {
	Tag uint16
}

func (e *MissingSchemaError) Error() string {
	return fmt.Sprintf("tag 0x%04X does not match any schemas", e.Tag)
}

// SchemaConversionError represents an error during schema conversion
type SchemaConversionError struct {
	Message string
}

func (e *SchemaConversionError) Error() string {
	return e.Message
}

// MessageTooLargeError represents an error when a message exceeds the maximum allowed size
type MessageTooLargeError struct {
	Size    int // Actual size of the message in bytes
	MaxSize int // Maximum allowed size in bytes
}

func (e *MessageTooLargeError) Error() string {
	return fmt.Sprintf("message size %d bytes exceeds maximum of %d bytes", e.Size, e.MaxSize)
}
