package smsg

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"testing"
)

func TestSmsgAdd(t *testing.T) {
	var r RawSMsg

	r.Add(0x1234, []byte("Hello"))
	r.Add(0x10, []byte("8"))

	if string(r.Data) != "12345 Hello00101 8" {
		t.Error(string(r.Data))
	}
}

func TestSmsgAddSafe(t *testing.T) {
	var r RawSMsg

	r.AddSafe(0x1234, []byte(""))
	r.AddSafe(0x10, []byte("\n"))
	r.AddSafe(0x11, []byte("123\n123\r\n"))

	if string(r.Data) != `12340 00102 \n001112 123\n123\r\n` {
		t.Errorf("\"%s\"\n", string(r.Data))
	}
}
func TestSmsgAddConstructed(t *testing.T) {
	var r RawSMsg

	r.Add(0x1234, []byte("Hello"))
	r.Add(0x10, []byte("8"))

	var varRaw1 RawSMsg

	varRaw1.AddVariableTag(0x1019)
	varRaw1.Add(0x10, []byte("8"))
	if string(varRaw1.Data) != "9019 00101 8" {
		t.Error(string(varRaw1.Data))
	}

	var varRaw2 RawSMsg

	varRaw2.AddRaw(0x1019, &r)
	varRaw2.Add(0x00, []byte{})
	if string(varRaw2.Data) != "901918 12345 Hello00101 800000 " {
		t.Error(string(varRaw2.Data))
	}

}

func tagEqual(t1, t2 Tag) bool {
	return t1.Tag == t2.Tag && t1.Constructor == t2.Constructor &&
		bytes.Compare(t1.Data, t2.Data) == 0
}
func TestIter(t *testing.T) {
	r := RawSMsg{[]byte("9019 922211 12345 Hello00101 800000 ")}
	exp := []Tag{
		Tag{Tag: 0x1019, Constructor: true, Data: []byte("922211 12345 Hello00101 800000 ")},
		Tag{Tag: 0x1222, Constructor: true, Data: []byte("12345 Hello")},
		Tag{Tag: 0x1234, Constructor: false, Data: []byte("Hello")},
		Tag{Tag: 0x0010, Constructor: false, Data: []byte("8")},
		Tag{Tag: 0x0000, Constructor: false, Data: []byte("")}}

	for i, it := 0, NewIter(&r); ; i++ {
		if tag, err := it.NextTag(); err != nil {
			if err == io.EOF && i == len(exp) {
				break
			}
			t.Errorf("t :%X err %v\n", tag.Tag, err)
			break
		} else if !tagEqual(tag, exp[i]) {
			t.Errorf("Got %s expected %s", &tag, &exp[i])
			break
		}
	}

}

func TestIterSkip(t *testing.T) {
	r := RawSMsg{[]byte("9019 922211 12345 Hello00101 800000 ")}
	exp := []Tag{
		Tag{Tag: 0x1019, Constructor: true, Data: []byte("922211 12345 Hello00101 800000 ")},
		Tag{Tag: 0x1234, Constructor: false, Data: []byte("Hello")},
		Tag{Tag: 0x0010, Constructor: false, Data: []byte("8")},
		Tag{Tag: 0x0000, Constructor: false, Data: []byte("")}}

	for i, it := 0, NewIter(&r); ; i++ {
		if tag, err := it.NextTag(); err != nil {
			if err == io.EOF && i == len(exp) {
				break
			}
			t.Errorf("t :%X err %v\n", tag.Tag, err)
			break
		} else {
			if tag.Tag == 0x1222 && tag.Constructor {
				if skerr := it.Skip(len(tag.Data)); skerr != nil {
					t.Logf("Error %v\n", skerr)
				}
			} else if !tagEqual(tag, exp[i]) {
				t.Errorf("Got %s expected %s", &tag, &exp[i])
				break
			}
		}
	}
}

func TestParseErr(t *testing.T) {
	r1 := RawSMsg{[]byte("10012 hi ")}

	i1 := NewIter(&r1)
	tag, err := i1.NextTag()
	if err != nil {
		t.Error(err)
	} else if tag.Tag != 0x1001 {
		t.Error(&t)
	}

	r2 := RawSMsg{[]byte("1001A hi ")}
	i2 := NewIter(&r2)
	tag, err = i2.NextTag()
	if err == nil {
		t.Error("expected error")
	}

	r3 := RawSMsg{[]byte("H0012 hi ")}
	i3 := NewIter(&r3)
	tag, err = i3.NextTag()
	if err == nil {
		t.Error("expected error")
	}

	r4 := RawSMsg{[]byte("1001-2 hi ")}
	i4 := NewIter(&r4)
	tag, err = i4.NextTag()
	if err != strconv.ErrRange {
		t.Error("expected error")
	}

	r5 := RawSMsg{[]byte("10014 hi ")}
	i5 := NewIter(&r5)
	tag, err = i5.NextTag()
	if err != io.ErrShortBuffer {
		t.Error("expected error")
	}

	r6 := RawSMsg{[]byte("10012hi")}
	i6 := NewIter(&r6)
	tag, err = i6.NextTag()
	if err != io.ErrShortBuffer {
		t.Error("expected error")
	}
}

func TestReader(t *testing.T) {
	msg := []byte("10015 hello \n")
	b := bytes.NewBuffer(msg)

	r := RawSMsgReader{bufio.NewReader(b)}

	ok, smsg, err := r.ReadRawSMsg()
	if !ok {
		t.Fail()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%v", smsg)

	ok, smsg, err = r.ReadRawSMsg()
	if ok {
		t.Fail()
	}
	if err != io.EOF {
		t.Fatal(err)
	}
	t.Logf("%v", smsg)

}
