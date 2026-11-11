package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fixedClock() func() time.Time {
	at := time.Date(2026, 11, 11, 17, 5, 9, 0, time.FixedZone("CET", 3600))
	return func() time.Time { return at }
}

func TestStampWriterStampsEveryLineOnce(t *testing.T) {
	stamp := fixedClock()().Local().Format(time.RFC3339) + " "
	cases := []struct {
		name   string
		writes []string
		want   string
	}{
		{"one line", []string{"dispatchqd: up\n"}, stamp + "dispatchqd: up\n"},
		{"two lines in one write", []string{"a\nb\n"}, stamp + "a\n" + stamp + "b\n"},
		{"line split across writes", []string{"dispatchqd: ", "run ", "failed\n"}, stamp + "dispatchqd: run failed\n"},
		{"unfinished line", []string{"a\nb"}, stamp + "a\n" + stamp + "b"},
		{"empty line", []string{"\n"}, stamp + "\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			w := &stampWriter{out: &out, now: fixedClock()}
			for _, s := range tc.writes {
				n, err := w.Write([]byte(s))
				if err != nil || n != len(s) {
					t.Fatalf("Write(%q) = %d, %v; want %d, nil", s, n, err, len(s))
				}
			}
			if out.String() != tc.want {
				t.Fatalf("wrote %q, want %q", out.String(), tc.want)
			}
		})
	}
}

// What the daemon prints through the swapped stream reaches the original file
// stamped, and all of it is there once the flush returns.
func TestStampStreamFlushesStampedLinesToTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dispatchqd.err.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	stream := f
	flush := stampStream(&stream, fixedClock())
	if flush == nil {
		t.Fatal("stampStream did not take the stream")
	}
	if stream == f {
		t.Fatal("the stream still points at the file")
	}
	for i := 0; i < 3; i++ {
		_, _ = fmt.Fprintf(stream, "dispatchqd: line %d\n", i)
	}
	flush()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	stamp := fixedClock()().Local().Format(time.RFC3339)
	want := fmt.Sprintf("%[1]s dispatchqd: line 0\n%[1]s dispatchqd: line 1\n%[1]s dispatchqd: line 2\n", stamp)
	if string(got) != want {
		t.Fatalf("log holds %q, want %q", got, want)
	}
}
