package gosmsg

import (
	"bytes"
	"errors"
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
	r.Terminate()
	if string(r.Data) != "12345 Hello00101 8000A0 0F072 \"\"00000 \n" {
		t.Error(string(r.Data))
	}
}

func TestSmsgAddTag(t *testing.T) {
	var r RawSMsg

	tags := []Tag{
		{Tag: 0x1019, Constructor: true, VarLen: true},
		{Tag: 0x1222, Data: []byte("hello")},
		{Tag: 0x0010, Data: []byte("8")},
	}
	r.AddTags(tags)
	if string(r.Data) != "9019 12225 hello00101 8" {
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
			if err == EOS && i == len(exp) {
				break
			}
			t.Errorf("t :%X err %v\n", tag.Tag, err)
			break
		} else if !tagEqual(&tag, &exp[i]) {
			t.Errorf("Got %s expected %s", &tag, &exp[i])
			break
		}
		if tag.VarLen {
			it = tag.SubTags()
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
	if err != EOS {
		t.Fatal(err)
	}
	t.Logf("%v", smsg)
}
func TestReaderMissingNewline(t *testing.T) {
	msg := []byte("10015 hello")
	b := bytes.NewBuffer(msg)

	r := NewRawSMsgReader(b)
	smsg, err := r.ReadRawSMsg()
	t.Logf("%s", smsg)
	if err != nil {
		t.Fatal(err)
	}
	// We should still get the data available
	if string(smsg.Data) != "10015 hello" {
		t.Fatalf("expected %s", msg)
	}

	smsg, err = r.ReadRawSMsg()
	if err != EOS {
		t.Fatal(err)
	}
}

// ============================================================================
// Edge Case Tests - Demonstrating Bugs
// ============================================================================

// TestParseErrNoSpaceAfterTag tests that NextTag properly handles tags at EOF.
// This test originally demonstrated Issue 3a where the function would panic.
// After the fix, it now properly returns io.ErrShortBuffer for incomplete tags.
func TestParseErrNoSpaceAfterTag(t *testing.T) {
	// Tag without space or data following it (exactly 4 bytes)
	r := RawSMsg{[]byte("1001")}

	it := r.Tags()
	_, err := it.NextTag()

	// Should return a proper error (io.ErrShortBuffer)
	if err == nil {
		t.Error("Expected error for tag without space/data, got nil")
	}
	if err != io.ErrShortBuffer {
		t.Errorf("Expected io.ErrShortBuffer, got: %v", err)
	}
	t.Logf("Error: %v", err)
}

// TestParseErrTagAtEOF tests various edge cases where tags are incomplete
// or malformed at end of message. All should return proper errors.
func TestParseErrTagAtEOF(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		desc string
	}{
		{
			name: "tag_only_no_space",
			data: []byte("1001"),
			desc: "Valid tag hex but no space after",
		},
		{
			name: "tag_with_partial_length",
			data: []byte("10011"),
			desc: "Tag followed by single digit but no space",
		},
		{
			name: "empty_after_tag",
			data: []byte("ABCD"),
			desc: "Valid hex tag but nothing following",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := RawSMsg{tt.data}
			it := r.Tags()
			_, err := it.NextTag()

			if err == nil {
				t.Errorf("Expected error for %s, got nil", tt.desc)
			}
			t.Logf("%s - Error (if no panic): %v", tt.desc, err)
		})
	}
}

// ============================================================================
// Message Size Limit Tests
// ============================================================================

// TestMessageSizeLimit tests that messages exceeding MaxMessageSize are rejected
func TestMessageSizeLimit(t *testing.T) {
	// Create a message just under the limit (100 bytes)
	smallMsg := make([]byte, 90)
	for i := range smallMsg {
		smallMsg[i] = 'A'
	}
	smallMsg = append(smallMsg, '\n')

	// Create a message over the limit (200 bytes, limit is 100)
	largeMsg := make([]byte, 190)
	for i := range largeMsg {
		largeMsg[i] = 'B'
	}
	largeMsg = append(largeMsg, '\n')

	// Combine messages
	buf := bytes.NewBuffer(append(smallMsg, largeMsg...))

	reader := NewRawSMsgReader(buf)
	reader.MaxMsgSize = 100 // Set custom limit

	// First message should succeed (under limit)
	msg1, err := reader.ReadRawSMsg()
	if err != nil {
		t.Fatalf("Expected first message to succeed, got error: %v", err)
	}
	if len(msg1.Data) != 90 {
		t.Errorf("Expected message length 90, got %d", len(msg1.Data))
	}

	// Second message should fail (over limit)
	_, err = reader.ReadRawSMsg()
	if err == nil {
		t.Fatal("Expected error for oversized message, got nil")
	}

	var tooLargeErr *MessageTooLargeError
	if !errors.As(err, &tooLargeErr) {
		t.Fatalf("Expected MessageTooLargeError, got: %T", err)
	}
	if tooLargeErr.MaxSize != 100 {
		t.Errorf("Expected MaxSize=100, got %d", tooLargeErr.MaxSize)
	}
}

// TestMessageSizeDefaultLimit tests that the default limit is applied
func TestMessageSizeDefaultLimit(t *testing.T) {
	// Create a small message
	smallMsg := []byte("10015 hello \n")

	buf := bytes.NewBuffer(smallMsg)
	reader := NewRawSMsgReader(buf)

	// Should have default limit set
	if reader.MaxMsgSize != DefaultMaxMsgSize {
		t.Errorf("Expected default limit %d, got %d", DefaultMaxMsgSize, reader.MaxMsgSize)
	}

	// Should succeed with small message
	_, err := reader.ReadRawSMsg()
	if err != nil {
		t.Fatalf("Expected success, got error: %v", err)
	}
}

// TestMessageSizeAtBoundary tests messages exactly at the size limit
func TestMessageSizeAtBoundary(t *testing.T) {
	limit := 100

	// Message exactly at limit (100 bytes including newline)
	atLimit := make([]byte, 99)
	for i := range atLimit {
		atLimit[i] = 'A'
	}
	atLimit = append(atLimit, '\n')

	buf := bytes.NewBuffer(atLimit)
	reader := NewRawSMsgReader(buf)
	reader.MaxMsgSize = limit

	// Should succeed (at limit, not exceeding)
	msg, err := reader.ReadRawSMsg()
	if err != nil {
		t.Fatalf("Expected success for message at limit, got error: %v", err)
	}
	// After stripping newline, should be 99 bytes
	if len(msg.Data) != 99 {
		t.Errorf("Expected message length 99, got %d", len(msg.Data))
	}
}

// ============================================================================
// Additional Error Path Tests - Common Real-World Scenarios
// ============================================================================

// TestIteratorWithCorruptedLength tests that the iterator handles corrupted
// length values gracefully without buffer overruns. This verifies the overflow
// protection added to NextTag.
func TestIteratorWithCorruptedLength(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr error
	}{
		{
			name:    "length_exceeds_remaining_data",
			data:    []byte("100110 abc"), // Claims 10 bytes but only has 3
			wantErr: io.ErrShortBuffer,
		},
		{
			name:    "huge_length_value",
			data:    []byte("10019999999 abc"), // Absurdly large length
			wantErr: io.ErrShortBuffer,
		},
		{
			name:    "length_equals_remaining",
			data:    []byte("10013 abc"), // Exactly 3 bytes available
			wantErr: nil,                 // Should succeed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := RawSMsg{tt.data}
			it := r.Tags()

			tag, err := it.NextTag()
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Expected error %v, got %v", tt.wantErr, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected success, got error: %v", err)
				}
				if tag.Tag != 0x1001 {
					t.Errorf("Expected tag 0x1001, got 0x%04X", tag.Tag)
				}
				if string(tag.Data) != "abc" {
					t.Errorf("Expected data 'abc', got %q", string(tag.Data))
				}
			}
		})
	}
}
