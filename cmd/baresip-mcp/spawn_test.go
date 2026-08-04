package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWatchBaresipOutput(t *testing.T) {
	// Mixed stream the way baresip emits it: ANSI-colored warnings, a
	// \r-redrawn status ticker with no newlines, and two DTMF failures.
	input := "[0:00:31] audio=0/63530 (bit/s)    \r" +
		"[0:00:32] audio=0/63530 (bit/s)    \r" +
		"\x1b[31mcall: sending DTMF INFO failed (scode: 500)\x1b[;m\n" +
		"some unrelated line\n" +
		"call: sending DTMF INFO failed (connection reset)\n"

	tmp := t.TempDir()
	logFile, err := os.Create(filepath.Join(tmp, "baresip.log"))
	if err != nil {
		t.Fatal(err)
	}

	pr, pw := io.Pipe()
	var details []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchBaresipOutput(pr, logFile, func(detail string) {
			details = append(details, detail)
		})
	}()

	if _, err := pw.Write([]byte(input)); err != nil {
		t.Fatal(err)
	}
	pw.Close()
	<-done

	if len(details) != 2 || details[0] != "scode: 500" || details[1] != "connection reset" {
		t.Fatalf("expected [scode: 500, connection reset], got %v", details)
	}

	// The tee must preserve the raw stream byte-for-byte.
	logged, err := os.ReadFile(filepath.Join(tmp, "baresip.log"))
	if err != nil {
		t.Fatal(err)
	}
	if string(logged) != input {
		t.Fatalf("log file diverges from raw stream:\n%q\nvs\n%q", logged, input)
	}
}

func TestWatchBaresipOutputNilCallback(t *testing.T) {
	tmp := t.TempDir()
	logFile, err := os.Create(filepath.Join(tmp, "baresip.log"))
	if err != nil {
		t.Fatal(err)
	}
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchBaresipOutput(pr, logFile, nil)
	}()
	if _, err := pw.Write([]byte("call: sending DTMF INFO failed (scode: 500)\n")); err != nil {
		t.Fatal(err)
	}
	pw.Close()
	<-done
}
