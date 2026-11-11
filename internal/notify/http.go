package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// defaultTimeout bounds every outbound channel. A notification is best-effort:
// a channel that stops answering must never hold up the daemon's send.
const defaultTimeout = 10 * time.Second

// errBodyLimit is how much of a rejected reply is quoted back in the error.
// Enough for the one-line reason these services return, not enough to fill the
// daemon log with an HTML error page.
const errBodyLimit = 200

// replyLimit is how much of an accepted reply is read for its receipt: the
// JSON these services answer with is a few hundred bytes.
const replyLimit = 4 << 10

// StatusError is a reply that refused the notification. A refusal for a
// reason on our side (a 4xx other than a timeout or rate limit: a revoked
// token, a deleted topic) will be refused again, so it is not worth retrying;
// a server error or a rate limit is.
type StatusError struct {
	Channel string
	Code    int
	Reason  string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s returned status %d%s", e.Channel, e.Code, e.Reason)
}

// Permanent reports whether err says the channel will refuse the same
// notification however often it is sent again.
func Permanent(err error) bool {
	var se *StatusError
	if !errors.As(err, &se) {
		return false
	}
	return se.Code/100 == 4 && se.Code != http.StatusRequestTimeout && se.Code != http.StatusTooManyRequests
}

// Refused reports whether err is the channel's answer refusing the message,
// as opposed to the message not reaching it (no network, a timeout).
func Refused(err error) bool {
	var se *StatusError
	return errors.As(err, &se)
}

// post sends one request to a channel's endpoint and turns a non-2xx reply into
// a [StatusError] naming the channel and quoting its reason, so a channel that
// stops accepting notifications says why in the daemon's log instead of failing
// silently. An accepted reply is returned (bounded) so the channel can pick its
// receipt out of it.
func post(ctx context.Context, client *http.Client, channel, endpoint, contentType, body string, header http.Header) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		// The parse error quotes the URL too; keep only its reason.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("build %s request: %w", channel, err)
	}
	req.Header.Set("Content-Type", contentType)
	for name, values := range header {
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		// The client's error quotes the whole URL, and for a Slack or Discord
		// webhook the URL is the secret. It is logged and kept in the outbox,
		// so only the host is named.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("%s request to %s: %w", channel, req.URL.Host, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return nil, &StatusError{Channel: channel, Code: resp.StatusCode, Reason: reason(resp.Body)}
	}
	// The channel accepted the message; a reply cut short only loses the
	// receipt, which is logged and never relied on.
	reply, _ := io.ReadAll(io.LimitReader(resp.Body, replyLimit))
	return reply, nil
}

// receiptField reads one string field out of an accepted JSON reply, or
// nothing when the reply is not JSON or lacks it: a receipt is only logged,
// never relied on.
func receiptField(reply []byte, field string) string {
	var doc map[string]any
	if json.Unmarshal(reply, &doc) != nil {
		return ""
	}
	v, _ := doc[field].(string)
	return v
}

// reason renders a failed reply's first line as a parenthesised suffix, or
// nothing when the channel said nothing.
func reason(r io.Reader) string {
	b, err := io.ReadAll(io.LimitReader(r, errBodyLimit))
	if err != nil {
		return ""
	}
	text := strings.TrimSpace(strings.ReplaceAll(string(b), "\n", " "))
	if text == "" {
		return ""
	}
	return " (" + text + ")"
}
