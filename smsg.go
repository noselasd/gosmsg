package gosmsg

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

type RawSMsg struct {
	Data []byte
}

const gConstructor uint16 = 0x8000
const gVariableLen = -2

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

// Add adds a new tag with the given data
// Note, newlines (\r or \n) must not occur within the data
func (s *RawSMsg) Add(tag uint16, data []byte) {
	s.addImpl(tag & ^gConstructor, len(data), data)
}

// AddVariableTag adds a new tag with a variable lenght tag
func (s *RawSMsg) AddVariableTag(tag uint16) {
	s.addImpl(tag|gConstructor, gVariableLen, []byte{})
}

// AddRaw adds the entire content of r as the value of a new tag
func (s *RawSMsg) AddRaw(tag uint16, r *RawSMsg) {
	s.addImpl(tag|gConstructor, len(r.Data), r.Data)
}

// AddTag adds a new tag
func (s *RawSMsg) AddTag(t *Tag) {
	if t.VarLen {
		s.addImpl(t.Tag|gConstructor, gVariableLen, t.Data)
	} else {
		s.Add(t.Tag, t.Data)
	}
}

// AddTags adds all the tags in t
// Tag.VarLen = True implies a constructor even if Tag.Constructor is false
func (s *RawSMsg) AddTags(t []Tag) {
	for i := range t {
		s.AddTag(&t[i])
	}
}

// Terminate ends the SMsg, adding the null tag and a newline
// Don't add more data after this
func (s *RawSMsg) Terminate() {
	s.addImpl(0x0000, 0, []byte{'\n'})
}

// AddSafe is a safe replacement for Add where newlines within data (\r or \n)
// is escaped.
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

// An Iter used to iterate through Tags
type Iter struct {
	data []byte
}

// A Tag of an SMsg
type Tag struct {
	Tag         uint16
	Constructor bool
	VarLen      bool
	Data        []byte
}

func (t *Tag) String() string {
	return fmt.Sprintf("Tag: 0x%04X C:%t Data:%s", t.Tag, t.Constructor, t.Data)
}

// Tags returns an iterator used to iterate all the tags in the SMsg
func (s *RawSMsg) Tags() Iter {
	return Iter{s.Data}
}

// SubTags iterate over tags stored in the data of the current tag
func (t *Tag) SubTags() Iter {
	return Iter{t.Data}
}

// NextTag returns the next Tag in the SMsg or an error.
// io.EOF is returned when there is no more tags to iterate
func (i *Iter) NextTag() (t Tag, err error) {
	if len(i.data) < 4 { //tag
		return t, io.EOF
	}

	tag, err := strconv.ParseUint(string(i.data[:4]), 16, 16)
	if err != nil {
		return t, err
	}

	i.data = i.data[4:]
	t.Constructor = uint16(tag)&gConstructor != 0
	t.Tag = uint16(tag) & ^gConstructor

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

		if dataStart+int(dataLen)+1 > len(i.data) {
			return t, io.ErrShortBuffer
		}

		data := i.data[dataStart+1:]
		t.Data = data[:int(dataLen)]
		// jump to next tag
		i.data = data[int(dataLen):]

	} else { //variable length
		i.data = i.data[1:]
		t.Data = i.data
		t.VarLen = true
	}

	return t, nil
}

// RawSMsgReader is used to read RawSMsgs from a stream.
type RawSMsgReader struct {
	//reader to read SMsgs from
	R         *bufio.Reader
	lastError error
}

// NewRawSMsgReader returns a new RawSMsgReader reading from r.
// r is wrapped in a *bufio.Reader unless it already is a *bufio.Reader
func NewRawSMsgReader(r io.Reader) RawSMsgReader {
	rr := RawSMsgReader{}
	if bufR, ok := r.(*bufio.Reader); ok {
		rr.R = bufR
	} else {
		rr.R = bufio.NewReader(r)
	}
	return rr
}

// ReadRawSMsg returns the next RawSmsg or an error.
// error will be io.EOF when the end is reached
// The returned RawSmsg could be empty if an empty line
// is encountered.
func (r *RawSMsgReader) ReadRawSMsg() (RawSMsg, error) {
	l, err := r.R.ReadBytes('\n')
	if r.lastError != nil {
		return RawSMsg{}, r.lastError
	}
	if len(l) > 0 {
		err = nil
		for _, b := range []byte("\r\n") {
			if len(l) > 0 && l[len(l)-1] == b {
				l = l[:len(l)-1]
			}
		}
	} else if err == nil {
		err = io.ErrUnexpectedEOF
	}

	r.lastError = err
	return RawSMsg{l}, err
}
