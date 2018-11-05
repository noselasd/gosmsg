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

func (s *RawSMsg) addImpl(tag uint16, len int, data []byte) {
	tagHex := []byte(fmt.Sprintf("%04X", tag))
	s.Data = append(s.Data, tagHex...)
	if len != gVariableLen {
		s.Data = strconv.AppendInt(s.Data, int64(len), 10)
	}
	s.Data = append(s.Data, ' ')

	s.Data = append(s.Data, data...)
}

func (s *RawSMsg) Add(tag uint16, data []byte) {
	s.addImpl(tag & ^gConstructor, len(data), data)
}

func (s *RawSMsg) AddVariableTag(tag uint16) {
	s.addImpl(tag|gConstructor, gVariableLen, []byte{})
}

func (s *RawSMsg) AddRaw(tag uint16, r *RawSMsg) {
	s.addImpl(tag|gConstructor, len(r.Data), r.Data)
}

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

type Iter struct {
	data []byte
}

type Tag struct {
	Tag         uint16
	Constructor bool
	Data        []byte
}

func (t *Tag) String() string {
	return fmt.Sprintf("Tag: 0x%04X C:%t Data:%s", t.Tag, t.Constructor, t.Data)
}

func (s *RawSMsg) Tags() Iter {
	return Iter{s.Data}
}

func (i *Iter) Skip(l int) error {
	if l > len(i.data) {
		return io.ErrShortBuffer
	}
	i.data = i.data[l:]
	return nil
}

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

		if t.Constructor {
			// assume nested tags, and start at the nested tag
			i.data = i.data[dataStart+1:]
			t.Data = i.data[:int(dataLen)]
		} else {
			//else, jump to next tag
			t.Data = i.data[dataStart+1 : dataStart+int(dataLen)+1]
			i.data = i.data[dataStart+1+int(dataLen):]

		}

	} else { //variable length
		i.data = i.data[1:]
		t.Data = i.data
	}

	return t, nil
}

type RawSMsgReader struct {
	R *bufio.Reader
}

//
func (r *RawSMsgReader) ReadRawSMsg() (bool, RawSMsg, error) {
	l, err := r.R.ReadBytes('\n')
	if len(l) > 0 {
		for _, b := range []byte("\r\n") {
			if len(l) > 0 && l[len(l)-1] == b {
				l = l[:len(l)-1]
			}
		}
	}

	return len(l) > 0, RawSMsg{l}, err
}
