package gosmsg

import (
	"bytes"
	"io"
	"strconv"
	"testing"
)

func TestSmsgAdd(t *testing.T) {
	var r RawSMsg

	r.Add(0x1234, []byte("Hello"))
	r.Add(0x10, []byte("8"))
	r.Add(0xA, []byte(""))
	r.Add(0x0F07, []byte("\"\""))

	if string(r.Data) != "12345 Hello00101 8000A0 0F072 \"\"" {
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

func tagEqual(t1, t2 *Tag) bool {
	return t1.Tag == t2.Tag && t1.Constructor == t2.Constructor && t1.VarLen == t2.VarLen &&
		bytes.Equal(t1.Data, t2.Data)
}
func TestIter(t *testing.T) {
	r := RawSMsg{[]byte("9019 922211 12345 Hello00101 800000 ")}
	exp := []Tag{
		{Tag: 0x1019, Constructor: true, VarLen: true, Data: []byte("922211 12345 Hello00101 800000 ")},
		{Tag: 0x1222, Constructor: true, VarLen: false, Data: []byte("12345 Hello")},

		{Tag: 0x0010, Constructor: false, VarLen: false, Data: []byte("8")},
		{Tag: 0x0000, Constructor: false, VarLen: false, Data: []byte("")}}
	expSubTag := Tag{Tag: 0x1234, Constructor: false, VarLen: false, Data: []byte("Hello")}

	inSub := false
	for i, it := 0, r.Tags(); ; i++ {
		tag, err := it.NextTag()
		if err != nil {
			if err == io.EOF && i == len(exp) {
				break
			}
			t.Errorf("t :%X err %v\n", tag.Tag, err)
			break
		} else if !tagEqual(&tag, &exp[i]) {
			t.Errorf("Got %s expected %s", &tag, &exp[i])
			break
		}

		if tag.Tag == 0x1222 && tag.Constructor {
			subIter := tag.SubTags()
			subTag, subErr := subIter.NextTag()
			if subErr != nil {
				t.Errorf("t :%X err %v\n", tag.Tag, err)
			}
			if !tagEqual(&subTag, &expSubTag) {
				t.Errorf("Got %s expected %s", &tag, &exp[i])
			}
			inSub = true
		}
	}

	if !inSub {
		t.Error("Not executed subTag code")
	}

}

func TestParseErr(t *testing.T) {
	r1 := RawSMsg{[]byte("10012 hi ")}

	i1 := r1.Tags()
	tag, err := i1.NextTag()
	if err != nil {
		t.Error(err)
	} else if tag.Tag != 0x1001 {
		t.Error(&t)
	}

	r2 := RawSMsg{[]byte("1001A hi ")}
	i2 := r2.Tags()
	tag, err = i2.NextTag()
	if err == nil {
		t.Error("expected error")
	}

	r3 := RawSMsg{[]byte("H0012 hi ")}
	i3 := r3.Tags()
	tag, err = i3.NextTag()
	if err == nil {
		t.Error("expected error")
	}

	r4 := RawSMsg{[]byte("1001-2 hi ")}
	i4 := r4.Tags()
	tag, err = i4.NextTag()
	if err != strconv.ErrRange {
		t.Error("expected error")
	}

	r5 := RawSMsg{[]byte("10014 hi ")}
	i5 := r5.Tags()
	tag, err = i5.NextTag()
	if err != io.ErrShortBuffer {
		t.Error("expected error")
	}

	r6 := RawSMsg{[]byte("10012hi")}
	i6 := r6.Tags()
	tag, err = i6.NextTag()
	if err != io.ErrShortBuffer {
		t.Error("expected error")
	}
}

func TestReader(t *testing.T) {
	msg := []byte("10015 hello \n10015 hello \n\n")
	b := bytes.NewBuffer(msg)

	r := NewRawSMsgReader(b)
	for i := 0; i < 2; i++ {
		smsg, err := r.ReadRawSMsg()

		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%v", smsg)
	}

	smsg, err := r.ReadRawSMsg()
	if err != nil {
		t.Fatal(err)
	}
	if len(smsg.Data) != 0 {
		t.Fatal("expected empty line")
	}

	smsg, err = r.ReadRawSMsg()

	if err != io.EOF {
		t.Fatal(err)
	}
	t.Logf("%v", smsg)

}
