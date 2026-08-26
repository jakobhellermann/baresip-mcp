package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
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

func TestRenderConfigLoadsSrtpBeforeAccount(t *testing.T) {
	cfg := renderConfig(configParams{
		sipListen:   "0.0.0.0:0",
		modPath:     "/mods",
		srtpModule:  "module                  srtp.so",
		audioModule: "module coreaudio.so",
		ctrlPort:    4444,
	})

	srtp := strings.Index(cfg, "srtp.so")
	account := strings.Index(cfg, "account.so")
	if srtp < 0 || account < 0 {
		t.Fatalf("expected both modules in config, got:\n%s", cfg)
	}
	// account.so resolves an account's mediaenc as it parses the accounts file.
	// Loaded first, it drops mediaenc=srtp-mand with a mere warning and the
	// account then rejects every RTP/SAVP offer it is sent.
	if srtp > account {
		t.Errorf("srtp.so must be loaded before account.so, got:\n%s", cfg)
	}
}

func TestRenderConfigOmitsAbsentSrtpModule(t *testing.T) {
	cfg := renderConfig(configParams{sipListen: "0.0.0.0:0", modPath: "/mods", ctrlPort: 4444})

	if strings.Contains(cfg, "srtp.so") {
		t.Errorf("expected no srtp module line, got:\n%s", cfg)
	}
	if !strings.Contains(cfg, "module                  account.so") {
		t.Errorf("expected account.so to survive an empty srtp line, got:\n%s", cfg)
	}
}
