package baresip

import (
	"regexp"
	"strings"
)

// ansiEscapeRE strips terminal color escapes that baresip embeds in reg output.
var ansiEscapeRE = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)

// Call is a parsed entry from the "listcalls" command output.
type Call struct {
	Line     int    `json:"line"`
	ID       string `json:"id"`
	Duration string `json:"duration"`
	State    string `json:"state"`
	OnHold   bool   `json:"on_hold"`
	PeerURI  string `json:"peer_uri"`
	Current  bool   `json:"current"`
}

// UserAgentCalls groups the parsed calls under a single UA.
type UserAgentCalls struct {
	UserAgent string `json:"user_agent"`
	Calls     []Call `json:"calls"`
}

// callLineRE matches a single line from baresip's ua_print_calls output, e.g.
//   "> [line 1, id deadbeef]  00:00:42  ESTABLISHED  (on hold)  sip:bob@x"
//   "  [line 2, id cafebabe]  00:00:01  RINGING                 sip:c@x"
var callLineRE = regexp.MustCompile(
	`^([>\s])\s*\[line\s+(\d+),\s*id\s+(\S+)\]\s+(\S+)\s+(\S+)\s+(\(on hold\)|\s{9})\s+(\S+)\s*$`)

// ParseListCalls parses the textual response of baresip's "listcalls"
// command into a per-UA structured representation. Unparseable lines are
// silently skipped — the goal is best-effort structure, not strict validation.
func ParseListCalls(s string) []UserAgentCalls {
	var out []UserAgentCalls
	var cur *UserAgentCalls

	for _, line := range strings.Split(s, "\n") {
		switch {
		case strings.HasPrefix(line, "User-Agent: "):
			ua := strings.TrimPrefix(line, "User-Agent: ")
			out = append(out, UserAgentCalls{UserAgent: ua})
			cur = &out[len(out)-1]
		case strings.HasPrefix(line, "--- Active calls"):
			// header; ignore
		default:
			m := callLineRE.FindStringSubmatch(line)
			if m == nil || cur == nil {
				continue
			}
			cur.Calls = append(cur.Calls, Call{
				Current:  m[1] == ">",
				Line:     atoi(m[2]),
				ID:       m[3],
				Duration: m[4],
				State:    m[5],
				OnHold:   strings.TrimSpace(m[6]) == "(on hold)",
				PeerURI:  m[7],
			})
		}
	}
	return out
}

// Registration is a parsed entry from the "reginfo" command output.
type Registration struct {
	Index    int    `json:"index"`
	AOR      string `json:"aor"`
	Status   string `json:"status"`            // "OK", "ERR", "zzz" (idle/initial), or "" if unparseable
	Fallback bool   `json:"fallback"`          // true if the registration is using the fallback server
	Server   string `json:"server,omitempty"`  // SIP server URI
	Expires  int    `json:"expires,omitempty"` // seconds; 0 if not present
}

// regLineRE matches one line of ua_print_status + reg_status output, e.g.:
//   "0 - sip:alice@example.com    OK  sip:srv.example.com  Expires 60s"
//   "1 - sip:bob@example.com      FB-ERR sip:fallback.example.com"
//   "0 - sip:carol@example.com    zzz"
// After ANSI stripping, the status token is one of: OK, ERR, zzz, optionally
// prefixed with "FB-" for fallback.
var regLineRE = regexp.MustCompile(
	`^(\d+)\s+-\s+(\S+)\s+(FB-)?(OK|ERR|zzz)(?:\s+(\S+))?(?:\s+Expires\s+(\d+)s)?\s*$`)

// ParseRegInfo parses the textual response of baresip's "reginfo" command.
// Lines that don't match the expected layout are skipped.
func ParseRegInfo(s string) []Registration {
	var out []Registration
	clean := ansiEscapeRE.ReplaceAllString(s, "")
	for _, line := range strings.Split(clean, "\n") {
		line = strings.TrimRight(line, " \t")
		// Collapse runs of whitespace so the regex stays simple.
		line = strings.Join(strings.Fields(line), " ")
		m := regLineRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		out = append(out, Registration{
			Index:    atoi(m[1]),
			AOR:      m[2],
			Fallback: m[3] == "FB-",
			Status:   m[4],
			Server:   m[5],
			Expires:  atoi(m[6]),
		})
	}
	return out
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return n
		}
		n = n*10 + int(c-'0')
	}
	return n
}
