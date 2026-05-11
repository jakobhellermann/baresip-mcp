package baresip

import (
	"regexp"
	"strings"
)

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
