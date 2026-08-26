package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const tmpDirPrefix = "baresip-mcp-"

// baresipInstance is a baresip child process we own. It writes its config
// and accounts into a tmpdir and shuts down cleanly on Close.
type baresipInstance struct {
	cmd     *exec.Cmd
	tmpDir  string
	logPath string
	addr    string
}

func (b *baresipInstance) Close() {
	if b == nil {
		return
	}
	if b.cmd != nil && b.cmd.Process != nil {
		// SIGTERM first so baresip can unregister from its provider and
		// flush its sockets. Fall back to SIGKILL after a short grace
		// period in case it ignored us.
		_ = b.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			_, _ = b.cmd.Process.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = b.cmd.Process.Kill()
			<-done
		}
	}
	if b.tmpDir != "" {
		_ = os.RemoveAll(b.tmpDir)
	}
}

// sweepOrphans kills any leftover baresip processes that were spawned by
// an earlier baresip-mcp and survived its (likely SIGKILL'd) parent. We
// can't rely on PR_SET_PDEATHSIG (Linux-only) and Claude Code does not
// reliably let us run our shutdown defer, so each new MCP starts by
// taking out the previous instances' stragglers.
//
// Only orphans (ppid=1) get touched. Tmpdirs of *live* baresips that
// belong to a sibling baresip-mcp must be left alone.
func sweepOrphans() {
	out, err := exec.Command("ps", "-eo", "pid,ppid,args").Output()
	if err != nil {
		return
	}
	tmpRoot := os.TempDir()
	killedDirs := map[string]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		pid, _ := strconv.Atoi(fields[0])
		ppid, _ := strconv.Atoi(fields[1])
		args := strings.Join(fields[2:], " ")
		if pid == 0 || ppid != 1 {
			continue
		}
		// Match only baresip processes started against one of our tmpdirs.
		if !strings.Contains(args, "baresip ") && !strings.HasSuffix(fields[2], "/baresip") {
			continue
		}
		dir := extractTmpDir(args, tmpRoot)
		if dir == "" {
			continue
		}
		log.Printf("sweeping orphan baresip pid=%d tmpdir=%s", pid, dir)
		_ = syscall.Kill(pid, syscall.SIGTERM)
		killedDirs[dir] = true
	}

	// Remove only the tmpdirs whose baresip we just killed. Other
	// baresip-mcp-* dirs may belong to live siblings (other Claude
	// sessions) and must be left alone.
	for dir := range killedDirs {
		_ = os.RemoveAll(dir)
	}
}

// extractTmpDir parses baresip's '-f <cfgdir>' argv and returns the
// parent tmpdir if it lives under tmpRoot and matches our prefix.
func extractTmpDir(args, tmpRoot string) string {
	idx := strings.Index(args, "-f ")
	if idx < 0 {
		return ""
	}
	rest := args[idx+3:]
	if end := strings.Index(rest, " "); end >= 0 {
		rest = rest[:end]
	}
	// rest looks like /<tmpRoot>/baresip-mcp-NNN/.baresip
	rest = strings.TrimSuffix(rest, "/.baresip")
	if !strings.HasPrefix(rest, filepath.Join(tmpRoot, tmpDirPrefix)) {
		return ""
	}
	return rest
}

// spawnParams configures a single baresip child. Use 0 for sipPort to
// let baresip pick any free port (recommended for additional instances
// once 5060 is taken by the first one). accountsLine is the literal one
// account-file line for this instance.
type spawnParams struct {
	sipPort      int    // 0 = baresip picks (use 5060 for the first instance to maximize NAT-pinhole stability)
	accountsLine string // single-account line; empty means baresip starts with zero UAs

	// onDTMFInfoFailure fires once per "call: sending DTMF INFO failed"
	// warning in the baresip output, with the parenthesized reason
	// (e.g. "scode: 500"). baresip only logs this — there is no ctrl_tcp
	// event — so the output stream is the only place to observe it.
	onDTMFInfoFailure func(detail string)
}

var (
	ansiSeqRE        = regexp.MustCompile("\x1b\\[[0-9;]*m")
	dtmfInfoFailedRE = regexp.MustCompile(`sending DTMF INFO failed \(([^)]*)\)`)
)

// watchBaresipOutput copies baresip's combined stdout/stderr into logFile
// while scanning it for the DTMF-INFO-failure warning. Owns both ends:
// closes them when the child's output hits EOF.
func watchBaresipOutput(r io.ReadCloser, logFile *os.File, onDTMFInfoFailure func(string)) {
	defer r.Close()
	defer logFile.Close()
	sc := bufio.NewScanner(io.TeeReader(r, logFile))
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	sc.Split(scanCROrLF)
	for sc.Scan() {
		if onDTMFInfoFailure == nil {
			continue
		}
		line := ansiSeqRE.ReplaceAllString(sc.Text(), "")
		for _, m := range dtmfInfoFailedRE.FindAllStringSubmatch(line, -1) {
			onDTMFInfoFailure(m[1])
		}
	}
}

// scanCROrLF splits on \n or \r: baresip's status ticker redraws via bare
// \r, which would otherwise accumulate into one endless line.
func scanCROrLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexAny(data, "\r\n"); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// withAccountParam appends ";<key>=<value>" to a baresip account line if
// that key isn't already present. Used by the fleet to inject defaults
// like ;answermode=auto;answerdelay=2 without touching the user's
// accounts file.
func withAccountParam(line, key, value string) string {
	if strings.Contains(line, ";"+key+"=") {
		return line
	}
	return line + ";" + key + "=" + value
}

// spawnBaresip writes a self-contained config + accounts into a tmpdir,
// starts baresip headless against it, waits until ctrl_tcp accepts
// connections, and returns the loopback address it's listening on.
func spawnBaresip(p spawnParams) (*baresipInstance, error) {
	modPath, err := detectModulePath()
	if err != nil {
		return nil, err
	}

	port, err := freeTCPPort()
	if err != nil {
		return nil, fmt.Errorf("find free tcp port: %w", err)
	}

	tmp, err := os.MkdirTemp("", tmpDirPrefix)
	if err != nil {
		return nil, err
	}
	// Generate audio assets that live in the tmpdir.
	greetingPath := filepath.Join(tmp, "greeting.wav")
	useGreeting := true
	if werr := writeGreetingWAV(greetingPath); werr != nil {
		log.Printf("greeting.wav generation failed (%v); falling back to silence", werr)
		useGreeting = false
	}
	// Prefer the WAV that ships with baresip ("the phone is ringing.")
	// because the same binary is known to play it correctly. Fall back to
	// our own generated tone cadence if the shipped one isn't installed.
	ringbackPath := findShippedSound("ringback.wav")
	useRingback := ringbackPath != ""
	if !useRingback {
		ringbackPath = filepath.Join(tmp, "ringback.wav")
		useRingback = true
		if werr := writeRingbackWAV(ringbackPath); werr != nil {
			log.Printf("ringback.wav generation failed (%v); ringback will be silent", werr)
			useRingback = false
		}
	}
	cfgDir := filepath.Join(tmp, ".baresip")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}

	// Pick a platform-native audio backend so the user actually hears the
	// remote side. ausine remains the source — we don't grab the mic
	// because multiple parallel baresips would all fight for it.
	audioModule, audioPlayer := platformAudio(modPath)

	sipListen := "0.0.0.0:0"
	if p.sipPort > 0 {
		sipListen = fmt.Sprintf("0.0.0.0:%d", p.sipPort)
	}
	ringbackLine := ""
	if useRingback {
		ringbackLine = "ringback_aufile         " + ringbackPath
	}
	if ringPath := findShippedSound("ring.wav"); ringPath != "" {
		ringbackLine += "\nring_aufile             " + ringPath
	}
	cfg := fmt.Sprintf(`# Generated by baresip-mcp — do not edit.
sip_listen              %s
module_path             %s
module                  g711.so
module                  auconv.so
module                  auresamp.so
module                  stun.so
module                  ice.so
module                  account.so
module                  fakevideo.so
module                  menu.so
module                  netroam.so
module                  ctrl_tcp.so
%s
ctrl_tcp_listen         127.0.0.1:%d
%s
%s
%s
audio_buffer            20-160
audio_buffer_mode       fixed
`, sipListen, modPath, audioModule, port, audioSourceLine(greetingPath, useGreeting), audioPlayer, ringbackLine)
	if err := os.WriteFile(filepath.Join(cfgDir, "config"), []byte(cfg), 0o644); err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}

	accountsBody := ""
	if p.accountsLine != "" {
		accountsBody = p.accountsLine + "\n"
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "accounts"), []byte(accountsBody), 0o644); err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}

	logPath := filepath.Join(tmp, "baresip.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}

	// Pipe the output through us instead of handing logFile to the child:
	// the watcher tees into logFile and scans for log-only warnings.
	pr, pw, err := os.Pipe()
	if err != nil {
		logFile.Close()
		_ = os.RemoveAll(tmp)
		return nil, err
	}

	cmd := exec.Command("baresip", "-f", cfgDir)
	cmd.Env = append(os.Environ(), "HOME="+tmp)
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		logFile.Close()
		_ = os.RemoveAll(tmp)
		return nil, fmt.Errorf("start baresip (is it on PATH?): %w", err)
	}
	// Close our copy of the write end so the watcher sees EOF when the
	// child exits.
	pw.Close()
	go watchBaresipOutput(pr, logFile, p.onDTMFInfoFailure)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if err := waitForCtrlTCP(addr, 5*time.Second); err != nil {
		// Surface baresip's log so the user has a chance at diagnosing.
		// The watcher goroutine owns logFile and closes it on EOF.
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		logBody, _ := os.ReadFile(logPath)
		_ = os.RemoveAll(tmp)
		return nil, fmt.Errorf("baresip ctrl_tcp on %s never came up: %w\n--- baresip log ---\n%s", addr, err, logBody)
	}

	return &baresipInstance{
		cmd:     cmd,
		tmpDir:  tmp,
		logPath: logPath,
		addr:    addr,
	}, nil
}

func freeTCPPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitForCtrlTCP(addr string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout after %s", d)
}

// findShippedSound looks for a WAV file in the well-known baresip share
// directories. Returns "" if not found.
func findShippedSound(name string) string {
	candidates := []string{
		"/opt/homebrew/share/baresip",
		"/usr/local/share/baresip",
		"/usr/share/baresip",
	}
	if p := os.Getenv("BARESIP_SHARE"); p != "" {
		candidates = append([]string{p}, candidates...)
	}
	// Brew installs into /opt/homebrew/Cellar/baresip/<ver>/share/baresip,
	// linked to /opt/homebrew/share/baresip. Glob for any version too.
	matches, _ := filepath.Glob("/opt/homebrew/Cellar/baresip/*/share/baresip")
	candidates = append(candidates, matches...)
	for _, d := range candidates {
		full := filepath.Join(d, name)
		if _, err := os.Stat(full); err == nil {
			return full
		}
	}
	return ""
}

// platformAudio returns the baresip module line and audio_player config
// appropriate for the current OS. Falls back silently if the expected
// module isn't present — the call still works, the user just won't hear
// the remote.
func audioSourceLine(greetingPath string, useGreeting bool) string {
	if useGreeting {
		return "module                  aufile.so\naudio_source            aufile," + greetingPath
	}
	// Fallback: silence via ausine at 0 Hz.
	return "module                  ausine.so\naudio_source            ausine,0"
}

func platformAudio(modPath string) (moduleLine, audioPlayer string) {
	type cand struct{ module, player string }
	var candidates []cand
	switch runtime.GOOS {
	case "darwin":
		candidates = []cand{
			{"coreaudio.so", "audio_player coreaudio,default"},
			{"audiounit.so", "audio_player audiounit,default"},
		}
	case "linux":
		candidates = []cand{
			{"alsa.so", "audio_player alsa,default"},
			{"pulse.so", "audio_player pulse,"},
		}
	}
	for _, c := range candidates {
		if _, err := os.Stat(filepath.Join(modPath, c.module)); err == nil {
			return "module " + c.module, c.player
		}
	}
	return "", ""
}

func detectModulePath() (string, error) {
	candidates := []string{
		"/opt/homebrew/lib/baresip/modules",
		"/usr/local/lib/baresip/modules",
		"/usr/lib/x86_64-linux-gnu/baresip/modules",
		"/usr/lib/aarch64-linux-gnu/baresip/modules",
		"/usr/lib/baresip/modules",
	}
	if p := os.Getenv("BARESIP_MODPATH"); p != "" {
		candidates = append([]string{p}, candidates...)
	}
	for _, c := range candidates {
		if _, err := os.Stat(filepath.Join(c, "ctrl_tcp.so")); err == nil {
			return c, nil
		}
	}
	return "", fmt.Errorf("baresip ctrl_tcp.so module not found in any known path; set BARESIP_MODPATH")
}
