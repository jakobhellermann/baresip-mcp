//go:build e2e

// Package e2e drives two real baresip processes against each other via
// ctrl_tcp to verify the baresip-mcp client end-to-end on a real call.
//
// Run with:
//   go test -tags e2e ./test/e2e/ -v
//
// Requires baresip on PATH. The module path is autodetected for
// homebrew (macOS) and debian-multiarch (linux); override with
// BARESIP_MODPATH.
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipgate/baresip-mcp/pkg/baresip"
)

func locateModulePath(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("BARESIP_MODPATH"); p != "" {
		return p
	}
	candidates := []string{
		"/opt/homebrew/lib/baresip/modules",
		"/usr/local/lib/baresip/modules",
		"/usr/lib/x86_64-linux-gnu/baresip/modules",
		"/usr/lib/aarch64-linux-gnu/baresip/modules",
		"/usr/lib/baresip/modules",
	}
	for _, c := range candidates {
		if _, err := os.Stat(filepath.Join(c, "ctrl_tcp.so")); err == nil {
			return c
		}
	}
	t.Skip("baresip ctrl_tcp.so module not found; set BARESIP_MODPATH")
	return ""
}

type instance struct {
	name    string
	cmd     *exec.Cmd
	homeDir string
	sipPort int
	ctrlAddr string
}

func startBaresip(t *testing.T, name string, sipPort, ctrlPort int, modPath, account string) *instance {
	t.Helper()
	home := t.TempDir()
	cfgDir := filepath.Join(home, ".baresip")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	cfg := fmt.Sprintf(`
sip_listen              127.0.0.1:%d
net_interface           127.0.0.1
module_path             %s
module                  g711.so
module                  ausine.so
module                  auconv.so
module                  auresamp.so
module                  account.so
module                  fakevideo.so
module                  menu.so
module                  ctrl_tcp.so
audio_buffer            20-160
audio_buffer_mode       fixed
ctrl_tcp_listen         127.0.0.1:%d
audio_source            ausine,440
`, sipPort, modPath, ctrlPort)
	if err := os.WriteFile(filepath.Join(cfgDir, "config"), []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	// baresip still requires at least one local UA to source the INVITE
	// from, even when dialing peer-to-peer by URI. regint=0 disables the
	// REGISTER request, so no provider/registrar is contacted.
	if err := os.WriteFile(filepath.Join(cfgDir, "accounts"), []byte(account+"\n"), 0o644); err != nil {
		t.Fatalf("write accounts: %v", err)
	}

	cmd := exec.Command("baresip", "-f", cfgDir)
	cmd.Env = append(os.Environ(), "HOME="+home)
	logFile, err := os.Create(filepath.Join(home, "baresip.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start baresip %s: %v", name, err)
	}
	inst := &instance{
		name:     name,
		cmd:      cmd,
		homeDir:  home,
		sipPort:  sipPort,
		ctrlAddr: fmt.Sprintf("127.0.0.1:%d", ctrlPort),
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		logFile.Close()
		if t.Failed() {
			data, _ := os.ReadFile(filepath.Join(home, "baresip.log"))
			t.Logf("--- %s baresip.log ---\n%s", name, data)
		}
	})
	return inst
}

func waitFor(t *testing.T, desc string, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func TestE2ECallBetweenTwoBaresips(t *testing.T) {
	modPath := locateModulePath(t)

	const (
		sipA, ctrlA = 25070, 24444
		sipB, ctrlB = 25071, 24445
	)

	startBaresip(t, "A", sipA, ctrlA, modPath,
		fmt.Sprintf("<sip:a@127.0.0.1:%d>;regint=0", sipA))
	startBaresip(t, "B", sipB, ctrlB, modPath,
		fmt.Sprintf("<sip:b@127.0.0.1:%d>;regint=0;catchall=yes", sipB))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Both ctrl_tcp listeners take a moment to come up. Retry briefly.
	dialCtrl := func(addr string) *baresip.Client {
		t.Helper()
		var lastErr error
		for i := 0; i < 50; i++ {
			c := baresip.New(addr)
			cctx, cc := context.WithTimeout(ctx, 500*time.Millisecond)
			err := c.Connect(cctx)
			cc()
			if err == nil {
				return c
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("connect %s: %v", addr, lastErr)
		return nil
	}

	clientA := dialCtrl(fmt.Sprintf("127.0.0.1:%d", ctrlA))
	defer clientA.Close()
	clientB := dialCtrl(fmt.Sprintf("127.0.0.1:%d", ctrlB))
	defer clientB.Close()

	// Drain events on both sides so the channels don't fill up.
	eventsA := make(chan baresip.Event, 64)
	go func() {
		for ev := range clientA.Events() {
			select {
			case eventsA <- ev:
			default:
			}
		}
	}()
	go func() {
		for range clientB.Events() {
		}
	}()

	// A dials B by URI — no registration, just direct INVITE to the SIP port.
	target := fmt.Sprintf("sip:e2e@127.0.0.1:%d", sipB)
	t.Logf("A dialing %s", target)
	resp, err := clientA.Do(ctx, "dial", target)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if !resp.OK {
		t.Fatalf("dial failed: %+v", resp)
	}

	// Wait for B to see an incoming call, then accept it.
	waitFor(t, "incoming call on B", 5*time.Second, func() bool {
		r, err := clientB.Do(ctx, "listcalls", "")
		if err != nil {
			return false
		}
		calls := baresip.ParseListCalls(r.Data)
		for _, ua := range calls {
			if len(ua.Calls) > 0 {
				return true
			}
		}
		return false
	})

	t.Log("B accepting call")
	resp, err = clientB.Do(ctx, "accept", "")
	if err != nil {
		t.Fatalf("accept on B: %v", err)
	}
	if !resp.OK {
		t.Fatalf("accept failed: %+v", resp)
	}

	// Wait for A to see the call as ESTABLISHED.
	waitFor(t, "call ESTABLISHED on A", 5*time.Second, func() bool {
		r, err := clientA.Do(ctx, "listcalls", "")
		if err != nil {
			return false
		}
		calls := baresip.ParseListCalls(r.Data)
		for _, ua := range calls {
			for _, c := range ua.Calls {
				if c.State == "ESTABLISHED" {
					return true
				}
			}
		}
		return false
	})

	t.Log("A hanging up")
	if _, err := clientA.Do(ctx, "hangup", ""); err != nil {
		t.Fatalf("hangup: %v", err)
	}

	// Verify the event stream on A saw both ESTABLISHED and CLOSED.
	var sawEstablished, sawClosed bool
	collectDeadline := time.After(3 * time.Second)
collect:
	for {
		select {
		case ev := <-eventsA:
			t.Logf("A event: class=%s type=%s param=%s", ev.Class, ev.Type, ev.Param)
			if strings.Contains(ev.Type, "ESTABLISHED") {
				sawEstablished = true
			}
			if strings.Contains(ev.Type, "CLOSED") {
				sawClosed = true
			}
			if sawEstablished && sawClosed {
				break collect
			}
		case <-collectDeadline:
			break collect
		}
	}
	if !sawEstablished {
		t.Error("A never saw an ESTABLISHED event")
	}
	if !sawClosed {
		t.Error("A never saw a CLOSED event")
	}
}
