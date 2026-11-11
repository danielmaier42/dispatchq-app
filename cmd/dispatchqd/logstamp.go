package main

import (
	"bytes"
	"io"
	"os"
	"sync"
	"time"
)

// stampWriter starts every line written through it with the local time
// (RFC 3339) and a space. The daemon log is read after the fact, often hours
// later; a line that does not say when it happened cannot be matched to a run.
type stampWriter struct {
	out     io.Writer
	now     func() time.Time
	midLine bool
}

// Write passes p on in one write, a timestamp inserted wherever a line
// begins. A line split across writes is stamped once, where it starts.
func (w *stampWriter) Write(p []byte) (int, error) {
	n := len(p)
	if n == 0 {
		return 0, nil
	}
	buf := make([]byte, 0, n+32)
	for len(p) > 0 {
		if !w.midLine {
			buf = w.now().Local().AppendFormat(buf, time.RFC3339)
			buf = append(buf, ' ')
			w.midLine = true
		}
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			buf = append(buf, p...)
			break
		}
		buf = append(buf, p[:i+1]...)
		p = p[i+1:]
		w.midLine = false
	}
	if _, err := w.out.Write(buf); err != nil {
		return 0, err
	}
	return n, nil
}

// stampStdio routes os.Stdout and os.Stderr through a stampWriter each, so
// every line the daemon prints carries its time without each of the many
// Fprintf calls having to say so. os.Stdout must stay an *os.File, so the
// streams become pipes, and a goroutine per stream copies what arrives to the
// original file with the stamps added. The returned func closes the pipes and
// waits until everything written to them has reached the files; call it last,
// right before the process exits. os.Stdout and os.Stderr are not put back, so
// nothing races a goroutine still printing: a write after it is lost.
//
// A stream that cannot be piped stays as it is. The runtime's own crash output
// goes to file descriptor 2 directly and is not stamped, which keeps it intact
// when the process dies before the copy could run.
func stampStdio(now func() time.Time) (restore func()) {
	var undo []func()
	for _, stream := range []**os.File{&os.Stdout, &os.Stderr} {
		if u := stampStream(stream, now); u != nil {
			undo = append(undo, u)
		}
	}
	return func() {
		for _, u := range undo {
			u()
		}
	}
}

func stampStream(stream **os.File, now func() time.Time) (restore func()) {
	orig := *stream
	if orig == nil {
		return nil
	}
	r, w, err := os.Pipe()
	if err != nil {
		return nil
	}
	*stream = w
	done := make(chan struct{})
	go func() {
		defer close(done)
		copyStamped(&stampWriter{out: orig, now: now}, r)
		_ = r.Close()
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = w.Close()
			<-done
		})
	}
}

// copyStamped copies src to dst until src ends. A failed write to dst drops
// that chunk and carries on: a log that cannot be written must not leave the
// pipe full and every later print in the daemon blocked on it.
func copyStamped(dst io.Writer, src io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			_, _ = dst.Write(buf[:n])
		}
		if err != nil {
			return
		}
	}
}
