package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPushoverSendsFailuresWithHighPriorityAndReturnsTheRequestID(t *testing.T) {
	var priorities []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		priorities = append(priorities, r.Form.Get("priority"))
		_, _ = io.WriteString(w, `{"status":1,"request":"5042853c-402d-4a18-abcb-168734a801de"}`)
	}))
	defer srv.Close()
	p := Pushover{Token: "tok", UserKey: "usr", URL: srv.URL}

	receipt, err := p.Deliver(context.Background(), Notification{Title: "t", Message: "m", Priority: PriorityHigh})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if receipt != "5042853c-402d-4a18-abcb-168734a801de" {
		t.Fatalf("receipt = %q", receipt)
	}
	if _, err := p.Deliver(context.Background(), Notification{Title: "t", Message: "m"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(priorities) != 2 || priorities[0] != "1" || priorities[1] != "" {
		t.Fatalf("priorities = %q, want high then none", priorities)
	}
}

func TestNtfySendsFailuresWithHighPriorityAndReturnsTheMessageID(t *testing.T) {
	var got []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Priority int `json:"priority"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		got = append(got, body.Priority)
		_, _ = io.WriteString(w, `{"id":"sPs71M8A2T","event":"message"}`)
	}))
	defer srv.Close()
	n := Ntfy{Server: srv.URL, Topic: "dispatchq"}

	receipt, err := n.Deliver(context.Background(), Notification{Title: "t", Message: "m", Priority: PriorityHigh})
	if err != nil || receipt != "sPs71M8A2T" {
		t.Fatalf("Deliver = %q, %v", receipt, err)
	}
	if _, err := n.Deliver(context.Background(), Notification{Title: "t", Message: "m"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(got) != 2 || got[0] != ntfyHighPriority || got[1] != 0 {
		t.Fatalf("priorities = %v, want high (4) then the default", got)
	}
}

func TestPermanentOnlyForRefusalsOnOurSide(t *testing.T) {
	cases := []struct {
		code int
		want bool
	}{
		{http.StatusBadRequest, true},
		{http.StatusUnauthorized, true},
		{http.StatusRequestTimeout, false},
		{http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false},
		{http.StatusBadGateway, false},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(c.code)
		}))
		_, err := post(context.Background(), nil, "pushover", srv.URL, "text/plain", "", nil)
		srv.Close()
		if got := Permanent(err); got != c.want {
			t.Errorf("status %d: Permanent = %v, want %v (err %v)", c.code, got, c.want, err)
		}
	}
	if Permanent(errors.New("dial tcp: lookup api.pushover.net: no such host")) {
		t.Error("a network error counts as permanent")
	}
	if !Permanent(fmt.Errorf("notify: %w", &StatusError{Channel: "ntfy", Code: 404})) {
		t.Error("a wrapped refusal is not recognised")
	}
}

// plainNotifier reports nothing but success or failure.
type plainNotifier struct{ err error }

func (p plainNotifier) Notify(context.Context, Notification) error { return p.err }

func TestDeliverFallsBackToNotify(t *testing.T) {
	boom := errors.New("boom")
	if receipt, err := Deliver(context.Background(), plainNotifier{err: boom}, Notification{}); receipt != "" || !errors.Is(err, boom) {
		t.Fatalf("Deliver = %q, %v", receipt, err)
	}
}
