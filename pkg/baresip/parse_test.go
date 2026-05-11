package baresip

import (
	"reflect"
	"testing"
)

func TestParseListCallsEmpty(t *testing.T) {
	got := ParseListCalls("")
	if got != nil {
		t.Fatalf("expected nil, got %#v", got)
	}
}

func TestParseRegInfo(t *testing.T) {
	// Reproduce the literal escape sequences baresip emits via print_scode.
	esc := "\x1b[32m" // green
	rst := "\x1b[;m"
	input := "\n--- User Agents (3) ---\n" +
		"0 - sip:alice@example.com                  " + esc + "OK " + rst + " sip:srv.example.com Expires 60s\n" +
		"1 - sip:bob@example.com                    FB-\x1b[31mERR\x1b[;m sip:fb.example.com\n" +
		"2 - sip:carol@example.com                  \x1b[33mzzz\x1b[;m\n" +
		"\n"

	got := ParseRegInfo(input)
	want := []Registration{
		{Index: 0, AOR: "sip:alice@example.com", Status: "OK", Server: "sip:srv.example.com", Expires: 60},
		{Index: 1, AOR: "sip:bob@example.com", Status: "ERR", Fallback: true, Server: "sip:fb.example.com"},
		{Index: 2, AOR: "sip:carol@example.com", Status: "zzz"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestParseRegInfoEmpty(t *testing.T) {
	if got := ParseRegInfo("\n--- User Agents (0) ---\n\n"); got != nil {
		t.Fatalf("expected nil, got %#v", got)
	}
}

func TestParseListCallsSingleUA(t *testing.T) {
	// Mirrors the exact format produced by ua_print_calls + call_info:
	// "%-42s" is not in listcalls; per-call lines come from call_info.
	input := "\nUser-Agent: alice@example.com\n" +
		"--- Active calls (2) ---\n" +
		"> [line 1, id deadbeef]  00:00:42  ESTABLISHED  (on hold)  sip:bob@example.com\n" +
		"  [line 2, id cafebabe]  00:00:01  RINGING               sip:carol@example.com\n" +
		"\n"

	got := ParseListCalls(input)
	want := []UserAgentCalls{
		{
			UserAgent: "alice@example.com",
			Calls: []Call{
				{Current: true, Line: 1, ID: "deadbeef", Duration: "00:00:42", State: "ESTABLISHED", OnHold: true, PeerURI: "sip:bob@example.com"},
				{Current: false, Line: 2, ID: "cafebabe", Duration: "00:00:01", State: "RINGING", OnHold: false, PeerURI: "sip:carol@example.com"},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mismatch\n got: %#v\nwant: %#v", got, want)
	}
}
