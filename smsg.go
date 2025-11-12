package gosmsg

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// RawSMsg represents a raw SMSG message containing tag-based binary data.
// Each message consists of tags in the format: TTTTLL DDDD where:
//   - TTTT: 4-digit hex tag (0x0000-0xFFFF)
//   - LL: Decimal length of data
//   - DDDD: Raw data bytes
//
// Messages are terminated with a 0x0000 tag followed by a newline.
type RawSMsg struct {
	Data []byte
}

const (
	// gConstructor is the bit flag (0x8000) that marks a tag as a constructor
	// (containing nested tags) rather than a simple value tag
	gConstructor uint16 = 0x8000

	// gVariableLen is a sentinel value indicating a tag has variable length
	// (no explicit length field, data extends to end of current scope)
	gVariableLen = -2
)

// gHex is a lookup table for fast hex digit conversion without allocations
var gHex = [...]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'}

// fast implementation
func appendHexTag(b []byte, v uint16) []byte {
	return append(b, gHex[(v&0xf000)>>12], gHex[(v&0xf00)>>8], gHex[(v&0x00f0)>>4], gHex[(v&0x000f)])
}

func (s *RawSMsg) addImpl(tag uint16, length int, data []byte) {
	//tagHex := []byte(fmt.Sprintf("%04X", tag))
	//buf := []byte{'0', '0', '0'}

	s.Data = appendHexTag(s.Data, tag)
	if length != gVariableLen {
		s.Data = strconv.AppendInt(s.Data, int64(length), 10)
	}
	s.Data = append(s.Data, ' ')
	s.Data = append(s.Data, data...)
}

// Add adds a new tag with the given data to the message.
// The tag parameter is the numeric tag identifier (0x0000-0xFFFF).
// The data parameter contains the raw bytes for this tag's value.
//
// Note: newlines (\r or \n) must not occur within the data. Use AddSafe if
// the data may contain newlines that need to be escaped.
func (s *RawSMsg) Add(tag uint16, data []byte) {
	s.addImpl(tag & ^gConstructor, len(data), data)
}

// AddVariableTag adds a new variable-length constructor tag to the message.
// Variable-length tags do not have an explicit length field; their data
// extends to the end of the current scope. The constructor bit (0x8000)
// is automatically set on the tag.
func (s *RawSMsg) AddVariableTag(tag uint16) {
	s.addImpl(tag|gConstructor, gVariableLen, []byte{})
}

// AddRaw adds the entire content of another RawSMsg as the value of a new
// constructor tag. This is useful for creating nested tag structures.
// The constructor bit (0x8000) is automatically set on the tag.
func (s *RawSMsg) AddRaw(tag uint16, r *RawSMsg) {
	s.addImpl(tag|gConstructor, len(r.Data), r.Data)
}

// AddTag adds a pre-constructed Tag to the message.
// If the tag has VarLen set to true, it will be added as a variable-length
// constructor tag. Otherwise, it is added as a regular tag.
func (s *RawSMsg) AddTag(t *Tag) {
	if t.VarLen {
		s.addImpl(t.Tag|gConstructor, gVariableLen, t.Data)
	} else {
		s.Add(t.Tag, t.Data)
	}
}

// AddTags adds multiple pre-constructed Tags to the message in sequence.
// For each tag, if VarLen is true, the constructor bit is set automatically.
func (s *RawSMsg) AddTags(t []Tag) {
	for i := range t {
		s.AddTag(&t[i])
	}
}

// Terminate ends the message by adding the null tag (0x0000) and a newline.
// No additional data should be added to the message after calling Terminate.
// This is required to properly delimit the message in the SMSG stream format.
func (s *RawSMsg) Terminate() {
	s.addImpl(0x0000, 0, []byte{'\n'})
}

// AddSafe is a safe replacement for Add that escapes newlines within data.
// Any \r or \n characters in the data are escaped as \r and \n respectively.
//
// Note: Backslashes are intentionally not escaped. This is format-level
// escaping to prevent breaking the message delimiter, not string-level escaping.
// The escaped sequences are not decoded back to actual newlines.
func (s *RawSMsg) AddSafe(tag uint16, data []byte) {
	r := make([]byte, 0, len(data))
	for _, c := range data {
		switch c {
		case '\r':
			c = 'r'
			r = append(r, '\\')
		case '\n':
			c = 'n'
			r = append(r, '\\')
		}

		r = append(r, c)
	}
	s.addImpl(tag, len(r), r)
}

// Iter is an iterator for traversing tags within an SMSG message.
// Create an iterator using RawSMsg.Tags() or Tag.SubTags().
type Iter struct {
	data []byte
}

// Tag represents a single tag within an SMSG message.
// Each tag consists of a numeric identifier, flags indicating its type,
// and the associated data bytes.
type Tag struct {
	Tag         uint16 // Numeric tag identifier (0x0000-0xFFFF)
	Constructor bool   // True if tag contains nested tags
	VarLen      bool   // True if tag has variable length (no explicit length field)
	Data        []byte // Raw data bytes for this tag (may contain nested tags if Constructor is true)
}

func (t *Tag) String() string {
	return fmt.Sprintf("Tag: 0x%04X C:%t Data:%s", t.Tag, t.Constructor, t.Data)
}

// Tags returns an iterator for traversing all tags in the message.
// Use this to parse the top-level tags of an SMSG message.
//
// Example:
//
//	for it := msg.Tags(); ; {
//	    tag, err := it.NextTag()
//	    if err == EOS {
//	        break
//	    }
//	    // process tag...
//	}
func (s *RawSMsg) Tags() Iter {
	return Iter{s.Data}
}

// SubTags returns an iterator for traversing nested tags within this tag's data.
// This is only meaningful for constructor tags (where Constructor is true).
// For regular value tags, this will iterate over the raw data bytes as if they
// were tags, which is typically not what you want.
func (t *Tag) SubTags() Iter {
	return Iter{t.Data}
}

// NextTag returns the next Tag in the message or an error.
// Returns EOS when there are no more tags to iterate.
// Returns io.ErrShortBuffer if the message is truncated or malformed.
//
// The returned Tag.Data slice references the underlying buffer directly
// without copying. Do not modify the original message data while using tags.
func (i *Iter) NextTag() (t Tag, err error) {
	if len(i.data) < 4 { //tag
		return t, EOS
	}

	tag, err := strconv.ParseUint(string(i.data[:4]), 16, 16)
	if err != nil {
		return t, err
	}

	i.data = i.data[4:]
	t.Constructor = uint16(tag)&gConstructor != 0
	t.Tag = uint16(tag) & ^gConstructor

	if len(i.data) == 0 {
		return t, io.ErrShortBuffer
	}

	if i.data[0] != ' ' {
		dataStart := bytes.IndexByte(i.data, ' ')
		if dataStart == -1 {
			return t, io.ErrShortBuffer
		}

		dataLen, err := strconv.ParseInt(string(i.data[:dataStart]), 10, 32)
		if err != nil {
			return t, err
		} else if dataLen < 0 {
			return t, strconv.ErrRange
		}

		// Check for integer overflow before performing arithmetic
		// This prevents potential buffer overruns with malicious input
		if dataLen > int64(len(i.data)-dataStart-1) {
			return t, io.ErrShortBuffer
		}

		if dataStart+int(dataLen)+1 > len(i.data) {
			return t, io.ErrShortBuffer
		}

		data := i.data[dataStart+1:]
		t.Data = data[:int(dataLen)]
		// jump to next tag
		i.data = data[int(dataLen):]

	} else { //variable length
		t.Data = i.data[1:]
		i.data = []byte{}
		t.VarLen = true
	}

	return t, nil
}

// RawSMsgReader reads SMSG messages from a stream.
// Messages are newline-delimited, with each line containing a complete SMSG.
//
// Important: RawSMsgReader is not safe for concurrent use by multiple goroutines.
// Each RawSMsgReader instance should only be used by a single goroutine at a time.
// If you need concurrent reading, create separate RawSMsgReader instances for each
// goroutine, each reading from a different underlying stream.
type RawSMsgReader struct {
	// R is the underlying buffered reader used to read SMSG messages
	R *bufio.Reader
}

// NewRawSMsgReader returns a new RawSMsgReader that reads from r.
// If r is already a *bufio.Reader, it is used directly; otherwise,
// r is wrapped in a new *bufio.Reader for efficient reading.
//
// The returned RawSMsgReader is not safe for concurrent use.
// Do not call ReadRawSMsg from multiple goroutines simultaneously.
func NewRawSMsgReader(r io.Reader) RawSMsgReader {
	rr := RawSMsgReader{}
	if bufR, ok := r.(*bufio.Reader); ok {
		rr.R = bufR
	} else {
		rr.R = bufio.NewReader(r)
	}
	return rr
}

// ReadRawSMsg returns the next RawSMsg from the stream or an error.
// Returns EOS when the end of the stream is reached.
// Returns ErrUnexpectedEnd if the stream ends unexpectedly.
//
// The returned RawSMsg may be empty if an empty line is encountered in the stream.
// Line endings (\r\n or \n) are automatically stripped from the returned message.
//
// If data is available when EOF is encountered, the data is returned with a nil error.
// The EOF will be returned on the subsequent call to ReadRawSMsg.
func (r *RawSMsgReader) ReadRawSMsg() (RawSMsg, error) {
	l, err := r.R.ReadBytes('\n')

	if len(l) > 0 {
		// Got data, strip line endings
		for _, b := range []byte("\r\n") {
			if len(l) > 0 && l[len(l)-1] == b {
				l = l[:len(l)-1]
			}
		}
		// If we got data with EOF, clear EOF (will appear on next read)
		if err == io.EOF {
			err = nil
		}
	} else if err == nil {
		// No data and no error = unexpected
		err = ErrUnexpectedEnd
	} else if err == io.EOF {
		// No data and EOF = end of stream
		err = EOS
	}

	return RawSMsg{l}, err
}
