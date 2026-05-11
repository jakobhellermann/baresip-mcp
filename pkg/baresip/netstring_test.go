package baresip

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func TestNetstringRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte(`{"command":"dial","params":"sip:alice@example.com"}`)
	if err := writeNetstring(&buf, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	want := "51:" + string(payload) + ","
	if buf.String() != want {
		t.Fatalf("encoded form mismatch\n got: %q\nwant: %q", buf.String(), want)
	}

	got, err := readNetstring(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch\n got: %q\nwant: %q", got, payload)
	}
}

func TestReadNetstringMissingComma(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("5:helloX"))
	if _, err := readNetstring(r); err == nil {
		t.Fatal("expected error for missing trailing comma")
	}
}

func TestReadNetstringBadLength(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("abc:hello,"))
	if _, err := readNetstring(r); err == nil {
		t.Fatal("expected error for non-numeric length")
	}
}
